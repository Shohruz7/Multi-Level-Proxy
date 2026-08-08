import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

import {
  backendUserData,
  loadGenUserData,
  proxyUserData,
} from './user-data';

/** Ports, in one place, because three constructs have to agree on each. */
const PORT = {
  /** What the NLB listens on, and what a client connects to. */
  edge: 443,
  /** The proxy's TLS + ALPN h2 listener. */
  proxy: 8443,
  /** The proxy's Prometheus scrape endpoint — also the health-check target. */
  metrics: 9090,
  /** The backends' h2c listener (docs/adr/0017). */
  backend: 8080,
} as const;

/** Graviton, because ADR 0006 picked aarch64 and the binary is built for it. */
const PROXY_INSTANCE = 'c7g.xlarge';
const BACKEND_INSTANCE = 'c7g.large';
/**
 * The load generator is deliberately *larger* than the proxy it drives.
 * Design doc §10.1: a saturated generator reports its own queueing delay as
 * server latency, and the resulting number is unfalsifiable. If the knee moves
 * when this instance grows, the knee was the generator.
 */
const LOADGEN_INSTANCE = 'c7g.2xlarge';

export class H2ProxyStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // ---------------------------------------------------------------- network
    //
    // A VPC this stack *creates*, never one it looks up. `Vpc.fromLookup` reads
    // the real account at synth time, which would make `cdk synth` require
    // credentials and a bootstrap — and this stack's whole validation story is
    // that it synthesizes and is template-tested without an AWS account
    // (docs/adr/0022). The same rule rules out `MachineImage.lookup`; the AL2023
    // helper below resolves through an SSM parameter at *deploy* time instead.
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 1,
      ipAddresses: ec2.IpAddresses.cidr('10.20.0.0/16'),
      subnetConfiguration: [
        { name: 'edge', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 20 },
        {
          name: 'backend',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 20,
        },
      ],
    });

    const machineImage = ec2.MachineImage.latestAmazonLinux2023({
      cpuType: ec2.AmazonLinuxCpuType.ARM_64,
    });

    // ------------------------------------------------------------- registries
    const proxyRepo = new ecr.Repository(this, 'ProxyImage', {
      imageScanOnPush: true,
      removalPolicy: RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });
    const backendRepo = new ecr.Repository(this, 'BackendImage', {
      imageScanOnPush: true,
      removalPolicy: RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });

    // ------------------------------------------------------------------ roles
    //
    // SSM Session Manager rather than SSH: no key pair to hold, no port 22 in
    // any security group, and the load test is driven over the same channel.
    const instanceRole = (scoped: string, repo: ecr.Repository) => {
      const role = new iam.Role(this, `${scoped}Role`, {
        assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'AmazonSSMManagedInstanceCore',
          ),
        ],
      });
      repo.grantPull(role);
      return role;
    };

    // -------------------------------------------------------- security groups
    const proxySg = new ec2.SecurityGroup(this, 'ProxySg', {
      vpc,
      description: 'h2proxyd: h2 listener and metrics, from inside the VPC only',
      allowAllOutbound: true,
    });
    const backendSg = new ec2.SecurityGroup(this, 'BackendSg', {
      vpc,
      description: 'h2c backends: reachable only from the proxies',
      allowAllOutbound: true,
    });
    const loadGenSg = new ec2.SecurityGroup(this, 'LoadGenSg', {
      vpc,
      description: 'load generator: no inbound at all, driven over SSM',
      allowAllOutbound: true,
    });

    // An NLB has no security group of its own, so the rule is written against
    // the VPC CIDR: what reaches this port is an NLB node forwarding a client,
    // or a health check from one. Nothing outside the VPC can route here.
    proxySg.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(PORT.proxy),
      'client traffic, forwarded by the NLB',
    );
    proxySg.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(PORT.metrics),
      'NLB health check and Prometheus scrapes',
    );
    backendSg.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(PORT.backend),
      'h2c from the proxies, via the internal NLB',
    );

    // --------------------------------------------------------------- backends
    const backendAsg = new autoscaling.AutoScalingGroup(this, 'Backends', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      instanceType: new ec2.InstanceType(BACKEND_INSTANCE),
      machineImage,
      securityGroup: backendSg,
      role: instanceRole('Backend', backendRepo),
      // No `desiredCapacity`: setting it makes every deploy reset the group to
      // that number, discarding whatever scaling had settled on. `minCapacity`
      // is the floor the group starts at.
      minCapacity: 2,
      maxCapacity: 4,
      requireImdsv2: true,
      userData: ec2.UserData.custom(
        backendUserData({
          registry: `${this.account}.dkr.ecr.${this.region}.amazonaws.com`,
          image: `${backendRepo.repositoryUri}:latest`,
          bodySize: 1024,
        }),
      ),
    });

    // Two backends, so that ejection has somewhere to send traffic and P2C has
    // something to choose between. One backend makes the load balancer and the
    // health system untestable by construction.
    const backendNlb = new elbv2.NetworkLoadBalancer(this, 'BackendNlb', {
      vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      crossZoneEnabled: true,
    });
    backendNlb
      .addListener('BackendListener', {
        port: PORT.backend,
        protocol: elbv2.Protocol.TCP,
      })
      .addTargets('BackendTargets', {
        port: PORT.backend,
        protocol: elbv2.Protocol.TCP,
        targets: [backendAsg],
        deregistrationDelay: Duration.seconds(10),
      });

    // ---------------------------------------------------------------- proxies
    const proxyAsg = new autoscaling.AutoScalingGroup(this, 'Proxies', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: new ec2.InstanceType(PROXY_INSTANCE),
      machineImage,
      securityGroup: proxySg,
      role: instanceRole('Proxy', proxyRepo),
      minCapacity: 2,
      maxCapacity: 4,
      requireImdsv2: true,
      associatePublicIpAddress: true,
      userData: ec2.UserData.custom(
        proxyUserData({
          registry: `${this.account}.dkr.ecr.${this.region}.amazonaws.com`,
          image: `${proxyRepo.repositoryUri}:latest`,
          backendDns: backendNlb.loadBalancerDnsName,
          // The thresholds `just calibrate` measured on legitimate laptop
          // traffic, passed as the environment variables they already are. They
          // are the *starting* point on real traffic, not the answer: a deployed
          // run is the first sight of real traffic shape, and re-calibrating
          // against it is a documented step, not an afterthought.
          guardEnv: {
            H2PROXYD_MAX_UPSTREAM_CONNS: '16',
            // Observe-only for the first run. A guard that trips on real
            // traffic before anyone has looked at real traffic turns a
            // mitigation into the outage it was meant to prevent.
            H2PROXYD_GUARD_OBSERVE_ONLY: '1',
          },
        }),
      ),
    });
    proxyAsg.node.addDependency(backendNlb);

    // ------------------------------------------------------------- the edge
    //
    // TCP, not TLS, and not an ALB. ADR 0005: an ALB — or a TLS listener here —
    // terminates HTTP/2 itself, so the proxy would never see a raw h2
    // connection and the layer this whole project exists to build would be
    // AWS's implementation instead of ours. If a future edit changes this
    // listener's protocol, the template test fails, which is the point of it.
    const nlb = new elbv2.NetworkLoadBalancer(this, 'Nlb', {
      vpc,
      internetFacing: true,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      // Off deliberately, for measurement rather than for availability: with
      // cross-zone on, a request from the same-AZ generator can be forwarded to
      // a proxy in the other AZ, and the inter-AZ RTT lands in the latency
      // histogram as if the proxy had spent it (§10.3).
      crossZoneEnabled: false,
    });

    nlb
      .addListener('Edge', { port: PORT.edge, protocol: elbv2.Protocol.TCP })
      .addTargets('ProxyTargets', {
        port: PORT.proxy,
        protocol: elbv2.Protocol.TCP,
        targets: [proxyAsg],
        deregistrationDelay: Duration.seconds(30),
        // Client IP preservation is what makes `x-forwarded-for` mean anything
        // (docs/adr/0021). Without it every request arrives from an NLB node
        // and the header records the load balancer, which no one needed.
        preserveClientIp: true,
        // An amendment to ADR 0005, which assumed a TCP health check. A TCP
        // check proves a socket accepted; it cannot tell a serving proxy from
        // one wedged after `accept`. Checking `/metrics` over HTTP proves the
        // process is scheduling and answering — and it avoids opening a TLS
        // handshake every few seconds that no one completes, which the daemon
        // would log as a failed handshake forever.
        healthCheck: {
          protocol: elbv2.Protocol.HTTP,
          port: String(PORT.metrics),
          path: '/metrics',
          interval: Duration.seconds(10),
          healthyThresholdCount: 2,
          unhealthyThresholdCount: 2,
        },
      });

    // -------------------------------------------------------- load generator
    //
    // In the VPC, in the same AZ as the first proxy subnet (§10.3): a generator
    // outside the VPC would measure the internet, and one in the other AZ would
    // measure the inter-AZ hop.
    const loadGen = new ec2.Instance(this, 'LoadGen', {
      vpc,
      vpcSubnets: { subnets: [vpc.publicSubnets[0]] },
      instanceType: new ec2.InstanceType(LOADGEN_INSTANCE),
      machineImage,
      securityGroup: loadGenSg,
      role: new iam.Role(this, 'LoadGenRole', {
        assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'AmazonSSMManagedInstanceCore',
          ),
        ],
      }),
      requireImdsv2: true,
      userData: ec2.UserData.custom(loadGenUserData()),
    });

    Tags.of(this).add('project', 'h2proxy');

    // ----------------------------------------------------------------- output
    new CfnOutput(this, 'EdgeDns', {
      value: nlb.loadBalancerDnsName,
      description: 'NLB DNS name — the h2load target, port 443',
    });
    new CfnOutput(this, 'ProxyRepositoryUri', {
      value: proxyRepo.repositoryUri,
      description: 'Push the aarch64 image here before scaling the ASG up',
    });
    new CfnOutput(this, 'BackendRepositoryUri', {
      value: backendRepo.repositoryUri,
      description: 'Push the backend image here',
    });
    new CfnOutput(this, 'LoadGenInstanceId', {
      value: loadGen.instanceId,
      description:
        'Connect with: aws ssm start-session --target <this>. Resolve the NLB to ' +
        'the AZ-local address first — cross-zone is off, so pinning the node is ' +
        'what keeps the run single-AZ.',
    });
  }
}
