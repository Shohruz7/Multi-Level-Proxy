/**
 * Instance bootstrap scripts.
 *
 * These are kept out of the stack file because they carry the half of the
 * week-8 tuning pass that is not a proxy setting: the kernel and file-descriptor
 * limits a loaded HTTP/2 intermediary needs from the host. On a laptop those are
 * already generous (`ulimit -n` is 1048576 on the dev machine); on a fresh
 * Amazon Linux instance they are not, and an accept loop that runs out of file
 * descriptors looks exactly like a proxy that got slow.
 */

/** Kernel tuning applied before anything starts listening. */
const SYSCTLS = `
# Accept queue: the default 4096 is smaller than the connection count the
# throughput profile opens in its first second, and an overflowed accept queue
# is a dropped SYN — measured at the client as latency the proxy never saw.
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# Ephemeral ports: the proxy is a *client* to the backends, and every pooled
# upstream connection takes one. The default range is ~28k.
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1

# Socket buffers: one connection window is 1 MiB (docs/adr/0014), and the kernel
# should not be the thing that caps a window we deliberately raised.
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 131072 16777216
net.ipv4.tcp_wmem = 4096 131072 16777216
`.trim();

/** Highest fd count a single process may hold. */
export const NOFILE = 1048576;

/**
 * Common prologue: kernel tuning, Docker, and an ECR login.
 *
 * `docker login` is done with the instance role rather than a stored
 * credential, which is why the role carries `ecr:GetAuthorizationToken`.
 */
function prologue(registry: string, image: string): string {
  return `#!/bin/bash
set -euxo pipefail

cat >/etc/sysctl.d/99-h2proxy.conf <<'SYSCTL'
${SYSCTLS}
SYSCTL
sysctl --system

dnf install -y docker awscli-2
systemctl enable --now docker

REGION=$(curl -sf -H "X-aws-ec2-metadata-token: $(curl -sf -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/placement/region)
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin ${registry}
docker pull ${image}
`;
}

/**
 * A systemd unit that runs one container on the host network.
 *
 * `--network host` on purpose: a bridge network would put a NAT between the
 * NLB and the proxy and the proxy would see the bridge address as the client
 * IP, which is precisely the header `x-forwarded-for` exists to carry
 * (docs/adr/0021). It also removes a hop from a path this project measures.
 *
 * The stop timeouts are ordered deliberately, and the order is the one the
 * daemon's own comment asks for: the drain deadline must stay under whatever
 * the runtime waits before SIGKILL, so
 * `H2PROXYD_DRAIN_DEADLINE < docker stop -t < TimeoutStopSec`. Get it backwards
 * and every deploy kills in-flight streams that the graceful drain (ADR 0018)
 * was written to finish.
 */
function unit(
  name: string,
  image: string,
  env: Record<string, string>,
  extraPre = '',
): string {
  const envFlags = Object.entries(env)
    .map(([k, v]) => `      -e ${k}=${v} \\`)
    .join('\n');
  return `
${extraPre}
cat >/etc/systemd/system/${name}.service <<UNIT
[Unit]
Description=${name}
After=docker.service
Requires=docker.service

[Service]
Restart=always
RestartSec=2
LimitNOFILE=${NOFILE}
TimeoutStopSec=60
ExecStartPre=-/usr/bin/docker rm -f ${name}
ExecStart=/usr/bin/docker run --rm --name ${name} \\
      --network host \\
      --ulimit nofile=${NOFILE}:${NOFILE} \\
${envFlags}
      ${image}
ExecStop=/usr/bin/docker stop -t 45 ${name}

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now ${name}
`;
}

/**
 * Proxy bootstrap.
 *
 * The interesting line is the one that resolves `backendDns`. `h2proxyd` parses
 * `H2PROXYD_UPSTREAMS` as `SocketAddr` — literal addresses only, never names —
 * and that is a deliberate decision recorded in its own doc comment: resolving
 * names at startup would hide a backend that moved behind a cache with no TTL
 * anyone chose. So the *deployment* does the resolution, once, visibly, at boot.
 *
 * That is only safe because the target is a Network Load Balancer: an NLB holds
 * one stable address per subnet for its lifetime. The same line in front of an
 * ALB would be a bug, because ALB addresses change underneath you.
 */
export function proxyUserData(opts: {
  registry: string;
  image: string;
  backendDns: string;
  guardEnv: Record<string, string>;
}): string {
  const resolve = `
UPSTREAMS=$(getent ahostsv4 ${opts.backendDns} | awk '{print $1}' | sort -u | sed 's/$/:8080/' | paste -sd, -)
test -n "$UPSTREAMS"
`;
  return (
    prologue(opts.registry, opts.image) +
    unit(
      'h2proxyd',
      opts.image,
      {
        H2PROXYD_LISTEN: '0.0.0.0:8443',
        H2PROXYD_METRICS: '0.0.0.0:9090',
        H2PROXYD_UPSTREAMS: '\\$UPSTREAMS',
        // Under the NLB's 45-second docker stop; see `unit` above.
        H2PROXYD_DRAIN_GRACE: '5',
        H2PROXYD_DRAIN_DEADLINE: '30',
        ...opts.guardEnv,
      },
      resolve,
    )
  );
}

/** Backend bootstrap: the same hyper h2c server the local dev loop uses. */
export function backendUserData(opts: {
  registry: string;
  image: string;
  bodySize: number;
}): string {
  return (
    prologue(opts.registry, opts.image) +
    unit('backend', opts.image, {
      BACKEND_LISTEN: '0.0.0.0:8080',
      BACKEND_BODY_SIZE: String(opts.bodySize),
    })
  );
}

/**
 * Load-generator bootstrap: h2load and nothing else.
 *
 * No service is installed. The generator is driven by hand over SSM Session
 * Manager, because a load test that starts itself at boot produces a number
 * nobody was watching.
 */
export function loadGenUserData(): string {
  return `#!/bin/bash
set -euxo pipefail

cat >/etc/sysctl.d/99-loadgen.conf <<'SYSCTL'
${SYSCTLS}
SYSCTL
sysctl --system

# h2load ships in the nghttp2 package on Amazon Linux 2023.
dnf install -y nghttp2

cat >/etc/security/limits.d/99-loadgen.conf <<'LIMITS'
* soft nofile ${NOFILE}
* hard nofile ${NOFILE}
LIMITS
`;
}
