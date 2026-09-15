# infra — the AWS stack, as code

A CDK app describing the deployment from design-doc §9.1: an internet-facing
**NLB passing TCP through** to an ASG of **Graviton** proxies, an internal NLB in
front of the h2c backends, and a same-AZ load generator.

**It has never been deployed.** There is no AWS account behind this project, and
nothing here should be read as a report from one. What is checked, on every push,
is that the app synthesizes and that the resulting template still says what it
must — see [docs/adr/0022](../docs/adr/0022-infrastructure-as-code.md) for why
that trade was made and what it costs.

```sh
just synth          # from the repo root: assertions + cdk synth, no account needed
cd infra && npm test    # the template assertions alone
```

## Why it synthesizes without credentials

The stack is **environment-agnostic** and does **no context lookups**. That is
not incidental — it is the property that makes it checkable here at all. One
`Vpc.fromLookup` or `MachineImage.lookup` reads the real account during
synthesis, and from then on the stack can only be validated by someone holding
credentials, which in practice means it stops being validated. A test asserts the
cloud assembly requires no context, so adding a lookup fails CI rather than
quietly removing the safety net.

## What the tests assert, and why each one

They are not coverage. Each catches a specific way this stack could deploy
perfectly and be wrong:

| Assertion | The failure it catches |
|---|---|
| Every listener is `Protocol: TCP` | Someone "adds TLS at the load balancer". Traffic still flows; the proxy stops seeing raw h2 frames, and AWS's HTTP/2 implementation replaces the one this project exists to build (ADR 0005). |
| Health check is HTTP `/metrics` on 9090 | Reverting to a TCP check, which cannot tell a serving proxy from one wedged after `accept`. |
| `preserve_client_ip.enabled` | `x-forwarded-for` recording an NLB node forever (ADR 0021) — invisible to every test inside the proxy. |
| Cross-zone disabled at the edge | An inter-AZ RTT landing in the latency histogram as if the proxy had spent it (§10.3). |
| All instance types are `c7g.*` | An x86 type against an `aarch64-musl` binary (ADR 0006) — fails only after a full deploy. |
| Generator is larger than the proxy | A saturated load generator reporting its own queueing delay as server latency (§10.1). |
| `LimitNOFILE` and the sysctls are present | The default fd limit. `h2proxyd`'s accept loop deliberately survives a transient accept failure, so nothing crashes to tell you — it just gets slow. |
| `drain deadline < docker stop < TimeoutStopSec` | Backwards, every deploy SIGKILLs the in-flight streams the graceful drain (ADR 0018) exists to finish. Shows up as a handful of 5xx per rollout. |
| `getent` resolves the upstreams | `H2PROXYD_UPSTREAMS` takes `SocketAddr`s, never names. A DNS name deploys cleanly and then fails to start. |
| No security group opens 22 or `0.0.0.0/0` | An SSH port nobody needs (access is over SSM), or a world-readable `/metrics` publishing every request rate and backend verdict in the project. |

## Deploying, and taking it down again

Never run. What follows is a procedure, not a report — every step is derived
from the stack rather than from experience, and the first real deploy should be
expected to find something.

**Order matters, and the first-deploy ordering is awkward on purpose.** The ECR
repositories are created by the same stack whose instances pull from them, so
there is no ordering in which the images already exist. The ASGs now carry a
`CreationPolicy`, which means a first deploy with empty repositories will *fail*
rather than silently bring up instances that never start — so push first.

```sh
# 0. Credentials, once. Never paste keys into a chat, including with an agent.
aws configure sso          # or aws configure

# 1. Bootstrap the account/region for CDK.
cdk bootstrap

# 2. Create only the registries, so there is somewhere to push to.
cdk deploy --exclusively -e H2ProxyStack   # see the note below if this fails

# 3. Build and push all three images, linux/arm64, from the repo root.
#    One Dockerfile builds all three: the deploy must not mix libc or toolchain.
REGION=$(aws configure get region)
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

for pkg in h2proxyd backend loadgen; do
  case "$pkg" in
    h2proxyd) repo=h2proxystack-proxyimage ;;   # use the stack's actual output
    backend)  repo=h2proxystack-backendimage ;;
    loadgen)  repo=h2proxystack-loadgenimage ;;
  esac
  docker buildx build --platform linux/arm64 \
    --build-arg PACKAGE="$pkg" \
    -t "$REGISTRY/$repo:latest" --push ..
done

# 4. Deploy for real.
cdk deploy

# 5. Drive the load from inside the VPC, over SSM.
aws ssm start-session --target "$(aws cloudformation describe-stacks \
  --stack-name H2ProxyStack --query \
  "Stacks[0].Outputs[?OutputKey=='LoadGenInstanceId'].OutputValue" --output text)"

# 6. **Tear it down.** See the cost note below for why this is a numbered step.
cdk destroy
```

If step 2 is awkward in practice — CDK has no clean "this resource only" mode
for a single stack — the alternative is to deploy with the ASG capacities at
zero, push, then raise them. Either way the invariant is the same: **images
before instances**.

### Running the measurement

On the generator, `loadgen` is on `PATH` as a wrapper around its container, so
the recipes in [../bench/README.md](../bench/README.md) read the same here as on
a laptop. Two things are specific to this rig:

* **Pin the AZ-local NLB address.** Cross-zone is off at the edge on purpose
  (§10.3), so resolving the DNS name round-robins across AZs and silently mixes
  an inter-AZ hop into the tail. Resolve it and pick the address in the
  generator's own AZ.
* **Quote the p99 from `loadgen`, never from h2load.** h2load is closed-loop and
  cannot measure a tail ([../bench/README.md](../bench/README.md)); it is on the
  box for peak-throughput runs, which is what it is good at.

Then **re-calibrate the abuse-guard thresholds**. The proxy ships with
`H2PROXYD_GUARD_OBSERVE_ONLY=1` because the committed thresholds were measured
on loopback laptop traffic; enforcing those against traffic nobody has looked at
yet is a mitigation behaving like an outage. Run `just calibrate`'s equivalent
here, read the headroom, then turn enforcement on.

### What it costs, and what keeps costing

Rough us-east-1 on-demand; check current pricing before relying on it.

| | Idle | Under load |
|---|---:|---:|
| 2× c7g.xlarge (proxies), 2× c7g.large (backends), 1× c7g.2xlarge (generator) | ~$0.72/h | ~$0.72/h |
| 2× NLB (edge + internal), hourly | ~$0.09/h | ~$0.09/h |
| NAT gateway, hourly | ~$0.045/h | ~$0.045/h |
| **NLB LCU**, at ~50k req/s × 1 KiB across *both* balancers | — | **~$3.7/h** |

The surprise is the last row: under real load the balancers cost several times
the instances, and traffic crosses two of them. An afternoon of genuine
saturation is plausibly $20–30, almost none of it the compute.

**What keeps billing if you walk away:** the NAT gateway (~$32/month at zero
traffic), two NLBs (~$32/month), five instances (~$590/month), their EBS
volumes, and any idle public IPv4 addresses. Note that scaling the ASGs to zero
by hand does **not** work — `minCapacity: 2` puts them back. `cdk destroy` is
the off switch, and the ECR repositories are `emptyOnDelete` so they go too.

A note on the NAT gateway, since it is the largest idle line item and the
obvious thing to remove: replacing it with VPC endpoints needs five interface
endpoints (ECR API, ECR Docker, S3 gateway, SSM, SSM Messages, EC2 Messages) at
about $0.01/h each, which is *more* per hour than the NAT it replaces and only
wins on per-GB egress. For a stack that exists for an afternoon and is then
destroyed, it is not worth the five extra resources and the extra bootstrap
failure mode. For one that stays up, it is. It is left as NAT deliberately.

## Layout

```
bin/h2proxy.ts        app entry — no `env`, deliberately
lib/h2proxy-stack.ts  the topology
lib/user-data.ts      host bootstrap: sysctls, fd limits, systemd units
test/stack.test.ts    the template assertions above
```
