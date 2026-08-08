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

## If an account ever appears

```sh
cdk bootstrap
cdk deploy                                  # outputs the ECR URIs and the NLB DNS
# build and push both images (see the repo Dockerfile — linux/arm64)
docker buildx build --platform linux/arm64 -t "$PROXY_URI:latest" --push ..
# then, over SSM on the load generator:
h2load -n 200000 -c 500 -m 20 --rate ... https://<AZ-local NLB address>/
```

Two things that would then need doing, and are not the deploy itself: pin the
generator to the **AZ-local** NLB address (cross-zone is off on purpose), and
**re-calibrate the abuse-guard thresholds** against real traffic shape. The
proxy ships with `H2PROXYD_GUARD_OBSERVE_ONLY=1` in its user-data for that
reason — a guard that enforces thresholds measured on loopback, against traffic
nobody has looked at yet, is a mitigation behaving like an outage.

## Layout

```
bin/h2proxy.ts        app entry — no `env`, deliberately
lib/h2proxy-stack.ts  the topology
lib/user-data.ts      host bootstrap: sysctls, fd limits, systemd units
test/stack.test.ts    the template assertions above
```
