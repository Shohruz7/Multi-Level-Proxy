#!/usr/bin/env node
//! CDK app entry point.
//!
//! The stack is deliberately **environment-agnostic**: no `env` is passed, so
//! account and region stay as CloudFormation pseudo-parameters. That is not a
//! stylistic choice — an environment-agnostic stack is forbidden from doing
//! context lookups, which is exactly what lets `cdk synth` run here with no AWS
//! account, no credentials and no bootstrap. See docs/adr/0022.
import { App } from 'aws-cdk-lib';

import { H2ProxyStack } from '../lib/h2proxy-stack';

const app = new App();

new H2ProxyStack(app, 'H2ProxyStack', {
  description:
    'h2proxy: NLB (TCP passthrough) -> Graviton proxy ASG -> internal NLB -> backend ASG, ' +
    'with a same-AZ load generator (design doc §10.3)',
});
