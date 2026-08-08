# ADR 0022 — Infrastructure as code: the stack shape, and why it is synth-validated rather than deployed

Status: accepted · Date: 2026-08-08 · Design doc: §9.1, §10.3 · Supersedes part of [0005](0005-nlb-self-terminated-tls.md)

## Context

Week 8 calls for the deployment: an ASG of Graviton instances behind an NLB, the
real container, a same-AZ load generator, and both load profiles run on real
hardware. The AWS account for it does not exist, and standing one up to run a
benchmark for an afternoon is a cost and a commitment this project does not
otherwise need.

The alternative that fails is worse than not deploying: writing a stack, never
exercising it, and describing it as though it ran. A CDK app that has never been
synthesized is not infrastructure as code — it is a long YAML-shaped opinion.

## Decision

**Write the full stack, and validate it by synthesis and template assertion
instead of by deploying it.** `infra/` is a TypeScript CDK app that produces a
complete CloudFormation template for the §9.1 topology, and `npm test` asserts
the properties of that template which a wrong edit would change silently.

The constraint that makes this possible, and the one most likely to be broken by
a future change: **the stack is environment-agnostic and does no context
lookups.** No `env` is passed to it, and it uses neither `Vpc.fromLookup` nor
`MachineImage.lookup`. A single one of those reads the real account during
synthesis, and from then on the stack cannot be checked without credentials and
a bootstrapped environment — which is how it would quietly stop being checked at
all. The AMI resolves instead through
`MachineImage.latestAmazonLinux2023({ cpuType: ARM_64 })`, which emits an SSM
parameter resolved at *deploy* time.

A test asserts the assembly requires no context, so the day someone adds a
lookup, CI says so.

### The topology

- **Internet-facing NLB, TCP listener on 443** → proxy ASG on 8443. Not TLS, not
  an ALB: ADR 0005's argument, now enforced by a template assertion rather than
  by memory.
- **Proxy ASG** — `c7g.xlarge` Graviton (ADR 0006), public subnets, running the
  `scratch` image from ECR under systemd on the host network.
- **Internal NLB → backend ASG** on 8080 h2c (ADR 0017), two backends minimum so
  that P2C balancing and outlier ejection have something to choose between.
- **Load generator** — one `c7g.2xlarge` in the same AZ as the first proxy
  subnet (§10.3), driven by hand over SSM.
- **SSM Session Manager, no SSH.** No key pair to hold or lose, and port 22 is
  open in nothing — asserted.

### Three decisions inside it that are not obvious

**The health check is HTTP on the metrics port, not TCP.** ADR 0005 assumed a
TCP health check because that is what an NLB does by default. A TCP check proves
a socket accepted; it cannot distinguish a serving proxy from one wedged after
`accept`, which is exactly the failure the project's own active PING probing
exists to catch on the *upstream* side. `GET /metrics` on 9090 proves the
process is scheduling and answering. It also stops the NLB opening a TLS
handshake every few seconds that nobody completes, which the daemon would log as
a failed handshake, forever.

**Cross-zone load balancing is off at the edge — for measurement, not for
availability.** With it on, a request from the same-AZ generator can be forwarded
to a proxy in the other AZ, and the inter-AZ RTT lands in the latency histogram
as though the proxy had spent it. §10.3 puts the generator in one AZ for a
reason; cross-zone would undo it. The run procedure has to pin the NLB's
AZ-local address for the same reason.

**The proxy's upstreams are resolved at boot, in user-data.** `h2proxyd` parses
`H2PROXYD_UPSTREAMS` as `SocketAddr` — literal addresses, never names — and that
is deliberate: resolving names at startup would hide a backend that moved behind
a cache with no TTL anyone chose. So the deployment does the resolution itself,
once, visibly, with `getent`. That is only sound because the target is an NLB,
which holds a stable address per subnet for its lifetime. The identical line in
front of an ALB would be a bug.

## Rejected alternatives

- **Deploy it.** The honest option, and the one the design doc assumes. Rejected
  on cost and account setup for a run that would last an afternoon. What is lost
  is real: instance-level numbers, the local-vs-deployed gap, and the first sight
  of real traffic shape for re-calibrating the abuse guard. `RESULTS.md` says so
  rather than implying laptop numbers are deployment numbers.
- **Write the stack and skip the tests**, on the grounds that an undeployed
  template is unverifiable anyway. But the assertions catch the class of error
  that matters here — a template that deploys *fine* and quietly does the wrong
  thing, like terminating TLS at the load balancer.
- **`cdk synth` alone as the gate.** It proves the app runs, not that it says
  anything in particular. Every property worth protecting would still be
  unguarded.
- **Terraform.** No argument against it; CDK is what the design doc named and
  what week 2 sketched.

## Consequences

- `just synth` and a CI job run `npm test` + `cdk synth` on every push, with no
  credentials. The stack cannot rot silently.
- Every claim about the deployment is a claim about a *template*. Nothing in this
  repository may say the stack was deployed, because it was not.
- If an account appears later, the remaining work is `cdk bootstrap`, two
  `docker push`es, and `cdk deploy` — plus the re-calibration of the guard
  thresholds against real traffic, which was always going to be a deployed step.
- The launch-template feature flag matters more than it looks: without
  `@aws-cdk/aws-autoscaling:generateLaunchTemplateInsteadOfLaunchConfig`, the ASGs
  synthesize as launch *configurations*, which AWS stopped offering to new
  accounts in 2023. The tests read `cdk.json` for exactly this reason — a bare
  `new App()` in a test synthesizes a different stack than `cdk deploy` would,
  and would have been asserting against one that cannot deploy.
