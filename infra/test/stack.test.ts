/**
 * Template assertions — this stack's substitute for having been deployed.
 *
 * The stack is written but never run (docs/adr/0022), so "it works" is not a
 * claim available here. What *is* available is that the template says what it
 * must say, and these tests assert the properties a wrong edit would change
 * silently — the ones where the resource still deploys, still passes its health
 * check, and quietly measures or protects the wrong thing.
 *
 * Each test names the failure it exists to catch. A template test that asserts
 * a property nothing would ever get wrong is a test that can only ever pass.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, it } from 'node:test';

import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';

import { H2ProxyStack } from '../lib/h2proxy-stack';

/**
 * Synthesize under the *same* context the CLI uses.
 *
 * Feature flags live in `cdk.json`, and only the CLI reads that file — a bare
 * `new App()` in a test synthesizes a different stack than `cdk deploy` would.
 * That is not academic: without
 * `generateLaunchTemplateInsteadOfLaunchConfig`, the ASGs here come out as
 * launch *configurations*, which AWS stopped offering to new accounts in 2023.
 * The tests would have been asserting against a stack that cannot deploy.
 */
function synth(): { template: Template; assembly: ReturnType<App['synth']> } {
  const { context } = JSON.parse(
    readFileSync(join(__dirname, '..', '..', 'cdk.json'), 'utf8'),
  ) as { context: Record<string, unknown> };
  const app = new App({ context });
  const stack = new H2ProxyStack(app, 'TestStack');
  return { template: Template.fromStack(stack), assembly: app.synth() };
}

const { template, assembly } = synth();

/**
 * Flatten a CloudFormation string expression back into text.
 *
 * User data reaches the template as `Fn::Base64(Fn::Join('', [...]))` with
 * unresolved tokens (`Ref`, `Fn::GetAtt`) interleaved between the literal
 * chunks, so it is neither a string nor base64 and cannot simply be decoded.
 * Tokens become `<token>`: what these tests check is the shell script we wrote,
 * not the addresses CloudFormation will substitute into it.
 */
function flatten(node: unknown): string {
  if (typeof node === 'string') return node;
  if (Array.isArray(node)) return node.map(flatten).join('');
  if (node && typeof node === 'object') {
    const obj = node as Record<string, unknown>;
    if (obj['Fn::Join']) {
      const [sep, parts] = obj['Fn::Join'] as [string, unknown[]];
      return parts.map(flatten).join(sep);
    }
    if (obj['Fn::Base64']) return flatten(obj['Fn::Base64']);
    return '<token>';
  }
  return '';
}

/**
 * Every instance type in the stack, by logical id.
 *
 * Two places have to be read, because the two constructs put it in different
 * ones: an ASG's type lives in its launch template, while a standalone
 * `ec2.Instance` keeps it on the instance and uses its launch template only to
 * carry the IMDSv2 requirement.
 */
function instanceTypes(): Record<string, string> {
  const found: Record<string, string> = {};
  for (const [id, res] of Object.entries(
    template.findResources('AWS::EC2::LaunchTemplate'),
  )) {
    const type = res.Properties.LaunchTemplateData.InstanceType;
    if (type) found[id] = type;
  }
  for (const [id, res] of Object.entries(
    template.findResources('AWS::EC2::Instance'),
  )) {
    found[id] = res.Properties.InstanceType;
  }
  return found;
}

/** The bootstrap script of the launch template whose logical id starts with `prefix`. */
function userDataOf(prefix: string): string {
  const found = Object.entries(
    template.findResources('AWS::EC2::LaunchTemplate'),
  ).find(([id]) => id.startsWith(prefix));
  assert.ok(found, `no launch template for ${prefix}`);
  return flatten(found[1].Properties.LaunchTemplateData.UserData);
}

describe('the edge', () => {
  it('passes TCP through instead of terminating HTTP/2 itself', () => {
    // The failure this catches: someone "adds TLS at the load balancer" and the
    // proxy stops seeing raw h2 frames. It would still serve traffic. It would
    // no longer be this project — ADR 0005.
    template.hasResourceProperties(
      'AWS::ElasticLoadBalancingV2::Listener',
      Match.objectLike({ Protocol: 'TCP', Port: 443 }),
    );
    const listeners = template.findResources(
      'AWS::ElasticLoadBalancingV2::Listener',
    );
    for (const [name, listener] of Object.entries(listeners)) {
      assert.equal(
        listener.Properties.Protocol,
        'TCP',
        `${name} must pass TCP through, not terminate it`,
      );
    }
  });

  it('is a network load balancer, not an application one', () => {
    template.hasResourceProperties(
      'AWS::ElasticLoadBalancingV2::LoadBalancer',
      Match.objectLike({ Type: 'network', Scheme: 'internet-facing' }),
    );
  });

  it('health-checks the proxy over HTTP on the metrics port', () => {
    // The failure this catches: reverting to a TCP health check, which cannot
    // tell a serving proxy from one wedged after accept().
    template.hasResourceProperties(
      'AWS::ElasticLoadBalancingV2::TargetGroup',
      Match.objectLike({
        Port: 8443,
        Protocol: 'TCP',
        HealthCheckProtocol: 'HTTP',
        HealthCheckPort: '9090',
        HealthCheckPath: '/metrics',
      }),
    );
  });

  it('preserves the client IP, so x-forwarded-for records a client', () => {
    // The failure this catches: XFF silently recording an NLB node forever
    // (docs/adr/0021), which no test inside the proxy can see.
    template.hasResourceProperties(
      'AWS::ElasticLoadBalancingV2::TargetGroup',
      Match.objectLike({
        Port: 8443,
        TargetGroupAttributes: Match.arrayWith([
          { Key: 'preserve_client_ip.enabled', Value: 'true' },
        ]),
      }),
    );
  });

  it('keeps the measurement single-AZ by disabling cross-zone at the edge', () => {
    // The failure this catches: an inter-AZ RTT landing in the latency
    // histogram as if the proxy had spent it (§10.3).
    const edge = Object.values(
      template.findResources('AWS::ElasticLoadBalancingV2::LoadBalancer', {
        Properties: { Scheme: 'internet-facing' },
      }),
    );
    assert.equal(edge.length, 1);
    assert.deepEqual(
      edge[0].Properties.LoadBalancerAttributes.find(
        (a: { Key: string }) => a.Key === 'load_balancing.cross_zone.enabled',
      ),
      { Key: 'load_balancing.cross_zone.enabled', Value: 'false' },
    );
  });
});

describe('the instances', () => {
  it('runs the proxy on Graviton, which is what the binary is built for', () => {
    // ADR 0006 targets aarch64-unknown-linux-musl. An x86 instance type here
    // would fail at `docker run`, but only after a full deploy.
    const types = Object.entries(instanceTypes());
    assert.equal(types.length, 3, 'proxy, backend and load generator');
    for (const [id, type] of types) {
      assert.match(type, /^c7g\./, `${id} runs ${type}, which is not Graviton`);
    }
  });

  it('drives the load from a bigger instance than it loads', () => {
    // §10.1: a saturated generator reports its own queueing delay as server
    // latency. Equal sizing is the classic way to publish a fake knee.
    const types = instanceTypes();
    const size = (t: string) =>
      ['large', 'xlarge', '2xlarge', '4xlarge'].indexOf(t.split('.')[1]);
    const of = (prefix: string) =>
      Object.entries(types).find(([id]) => id.startsWith(prefix))![1];
    assert.ok(
      size(of('LoadGen')) > size(of('Proxies')),
      `generator ${of('LoadGen')} must outsize proxy ${of('Proxies')}`,
    );
  });

  it('raises the file-descriptor limit on the proxy hosts', () => {
    // The failure this catches: the default 1024 fds. An accept loop that runs
    // out of descriptors looks exactly like a proxy that got slow, and
    // h2proxyd's accept loop deliberately survives it
    // (h2proxyd/src/main.rs — "a transient accept error must not take down the
    // whole listener"), so nothing crashes to tell you.
    const decoded = userDataOf('Proxies');
    assert.match(decoded, /LimitNOFILE=1048576/);
    assert.match(decoded, /--ulimit nofile=1048576:1048576/);
    assert.match(decoded, /net\.core\.somaxconn = 65535/);
  });

  it('stops the container more slowly than the proxy drains', () => {
    // The ordering the daemon's own comment asks for: the drain deadline must
    // stay under whatever the runtime waits before SIGKILL. Backwards, and
    // every deploy kills the in-flight streams ADR 0018 exists to finish —
    // visible only as a handful of 5xx during a rollout.
    const decoded = userDataOf('Proxies');
    const deadline = Number(/H2PROXYD_DRAIN_DEADLINE=(\d+)/.exec(decoded)![1]);
    const dockerStop = Number(/docker stop -t (\d+)/.exec(decoded)![1]);
    const systemd = Number(/TimeoutStopSec=(\d+)/.exec(decoded)![1]);
    assert.ok(
      deadline < dockerStop && dockerStop < systemd,
      `expected drain(${deadline}) < docker(${dockerStop}) < systemd(${systemd})`,
    );
  });

  it('resolves the backend NLB to literal addresses at boot', () => {
    // h2proxyd parses H2PROXYD_UPSTREAMS as SocketAddr — names are rejected on
    // purpose. Handing it a DNS name deploys cleanly and then fails to start.
    const decoded = userDataOf('Proxies');
    assert.match(decoded, /getent ahostsv4/);
    assert.match(decoded, /H2PROXYD_UPSTREAMS=/);
  });
});

describe('the security groups', () => {
  it('opens no SSH anywhere', () => {
    // Access is over SSM Session Manager; a key pair is a thing to lose.
    const groups = template.findResources('AWS::EC2::SecurityGroup');
    for (const [name, sg] of Object.entries(groups)) {
      for (const rule of sg.Properties.SecurityGroupIngress ?? []) {
        assert.notEqual(rule.FromPort, 22, `${name} opens SSH`);
      }
    }
    const standalone = template.findResources('AWS::EC2::SecurityGroupIngress');
    for (const [name, rule] of Object.entries(standalone)) {
      assert.notEqual(rule.Properties.FromPort, 22, `${name} opens SSH`);
    }
  });

  it('exposes the proxy and its metrics to the VPC only', () => {
    // The failure this catches: 0.0.0.0/0 on 9090, which publishes every
    // request rate, backend health verdict and guard threshold in the project.
    const groups = Object.values(template.findResources('AWS::EC2::SecurityGroup'));
    const open = groups.flatMap((sg) =>
      (sg.Properties.SecurityGroupIngress ?? []).filter(
        (r: { CidrIp?: string }) => r.CidrIp === '0.0.0.0/0',
      ),
    );
    assert.deepEqual(open, [], 'no ingress rule may be world-open');
  });
});

describe('synthesis without an AWS account', () => {
  it('needs no context lookup', () => {
    // This is the assertion the whole validation story rests on. A single
    // `Vpc.fromLookup` or `MachineImage.lookup` would make `cdk synth` demand
    // credentials and a bootstrapped environment, and the stack would become
    // unverifiable here — which is how it would quietly stop being checked.
    assert.deepEqual(
      assembly.manifest.missing ?? [],
      [],
      'the stack must not require environmental context',
    );
    template.resourceCountIs('AWS::EC2::VPC', 1);
  });

  it('resolves the AMI at deploy time, through SSM', () => {
    // The AMI arrives as an SSM-parameter-typed CloudFormation parameter, which
    // is what `MachineImage.latestAmazonLinux2023` produces and
    // `MachineImage.lookup` does not — the latter would read the account at
    // synth time and break the test above.
    const ssm = Object.values(template.findParameters('*')).filter((p) =>
      String(p.Type).startsWith('AWS::SSM::Parameter::Value'),
    );
    assert.ok(
      ssm.some((p) => String(p.Default).includes('al2023-ami')),
      'the AL2023 arm64 AMI should resolve through SSM at deploy time',
    );
  });
});
