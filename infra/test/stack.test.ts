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
import { execFileSync } from 'node:child_process';
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

/**
 * The bootstrap script of the resource whose logical id starts with `prefix`.
 *
 * Two places again, and for the same reason as `instanceTypes`: an ASG's user
 * data lives on its launch template, while a standalone `ec2.Instance` keeps it
 * on the instance and uses its launch template only to carry the IMDSv2
 * requirement. Reading only launch templates finds an empty one for the load
 * generator and reports nothing rather than failing, which is the worst of the
 * available outcomes.
 */
function userDataOf(prefix: string): string {
  const template_ = Object.entries(
    template.findResources('AWS::EC2::LaunchTemplate'),
  ).find(
    ([id, res]) =>
      id.startsWith(prefix) && res.Properties.LaunchTemplateData.UserData,
  );
  if (template_) {
    return flatten(template_[1].Properties.LaunchTemplateData.UserData);
  }
  const instance = Object.entries(
    template.findResources('AWS::EC2::Instance'),
  ).find(([id]) => id.startsWith(prefix));
  assert.ok(instance, `no user data for ${prefix}`);
  return flatten(instance[1].Properties.UserData);
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

  it('writes the resolved addresses into the unit, not the literal $UPSTREAMS', () => {
    // The assertion above passes whether the value is right or not, and for a
    // while it was wrong: the unit file is written by an *unquoted* heredoc,
    // where a backslash-dollar suppresses expansion, so `\$UPSTREAMS` put the
    // seven-character string `$UPSTREAMS` into ExecStart. systemd does not run
    // ExecStart through a shell — it substitutes from the unit's own
    // Environment=, this unit sets none, and an unset unbraced variable expands
    // to *zero words*. `docker run` then received a dangling `-e`, failed,
    // restarted forever, and never answered /metrics. Nothing in CI noticed,
    // because the script was only ever matched as text.
    //
    // So this test stops reading the script and runs it. Only the heredoc that
    // writes the unit is extracted and evaluated — the rest installs packages
    // and talks to ECR — which is enough to pin the escaping, the thing that
    // was wrong and the thing a future edit would get wrong the same way.
    const decoded = userDataOf('Proxies');
    const heredoc = decoded.match(
      /cat >\/etc\/systemd\/system\/h2proxyd\.service <<UNIT\n([\s\S]*?)\nUNIT\n/,
    );
    assert.ok(heredoc, 'the proxy unit must be written by a UNIT heredoc');

    const addresses = '10.20.1.5:8080,10.20.2.7:8080';
    const rendered = execFileSync(
      'bash',
      ['-c', `UPSTREAMS=${JSON.stringify(addresses)}\ncat <<UNIT\n${heredoc[1]}\nUNIT\n`],
      { encoding: 'utf8' },
    );

    assert.ok(
      rendered.includes(`H2PROXYD_UPSTREAMS=${addresses}`),
      `the unit must carry the addresses resolved at boot, got:\n${rendered}`,
    );
    assert.ok(
      !rendered.includes('$UPSTREAMS'),
      'an unexpanded $UPSTREAMS in ExecStart is the bug this test exists for',
    );
  });
});

describe('failing loudly', () => {
  it('fails the deploy when a bootstrap fails, rather than reporting success', () => {
    // Without a CreationPolicy, CloudFormation considers an ASG created as soon
    // as the API call returns. An instance whose user data died still passes its
    // EC2 status checks forever, so the group never replaces it, the target
    // group sits at zero healthy, and `cdk deploy` prints CREATE_COMPLETE. That
    // is how the $UPSTREAMS bug above could have survived a real deploy.
    const groups = template.findResources('AWS::AutoScaling::AutoScalingGroup');
    const ids = Object.keys(groups);
    assert.equal(ids.length, 2, 'proxies and backends');
    for (const [id, res] of Object.entries(groups)) {
      assert.ok(
        res.CreationPolicy?.ResourceSignal,
        `${id} must wait for cfn-signal, or a broken bootstrap deploys green`,
      );
    }
  });

  it('lets the load balancer, not just EC2, decide an instance is healthy', () => {
    // EC2 status checks say the virtual machine is alive. They say nothing about
    // whether the container inside it ever started, which is the failure that
    // actually happens here.
    for (const [id, res] of Object.entries(
      template.findResources('AWS::AutoScaling::AutoScalingGroup'),
    )) {
      const types: string[] = res.Properties.HealthCheckType
        ? [res.Properties.HealthCheckType]
        : [];
      assert.ok(
        types.includes('ELB'),
        `${id} must take the load balancer's word for health, got ${JSON.stringify(types)}`,
      );
    }
  });
});

describe('the load generator', () => {
  it('ships loadgen, not just h2load', () => {
    // h2load is closed-loop and structurally cannot measure a tail
    // (bench/README.md). A rig that can only run h2load cannot produce the p99
    // this project reports, and for a while that is exactly what this stack
    // deployed: `dnf install -y nghttp2` and nothing else.
    const decoded = userDataOf('LoadGen');
    assert.match(decoded, /nghttp2/, 'h2load is still wanted for throughput');
    assert.match(
      decoded,
      /\/usr\/local\/bin\/loadgen/,
      'loadgen must be on PATH, or the tail is unmeasurable from here',
    );
  });

  it('can pull the image it is told to run', () => {
    // The role used to carry SSM only, so the generator could be handed an image
    // reference it had no permission to fetch.
    template.resourceCountIs('AWS::ECR::Repository', 3);
    const policies = template.findResources('AWS::IAM::Policy');
    const pulls = Object.values(policies).filter((p) =>
      JSON.stringify(p.Properties.PolicyDocument).includes(
        'ecr:BatchGetImage',
      ),
    );
    assert.equal(
      pulls.length,
      3,
      'proxy, backend and generator each need pull on their own repository',
    );
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
