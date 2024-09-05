# Advanced Nats Streaming

![img](https://img.shields.io/badge/License-EPL_2.0-blue.svg) ![img](https://img.shields.io/badge/code--coverage-70%25-green)
[![Actions Status](https://github.com/imperiuse/advanced-nats-client/v1/workflows/Test/badge.svg)](https://github.com/imperiuse/advanced-nats-client/v1/actions)

An advanced NATS and NATS-Streaming client library based on the nats.go and stun.go projects.

I aim to combine all the advantages and minimize the drawbacks of both NATS and NATS Streaming.

For example, as you know, NATS Streaming does not support the Request-Reply pattern, while a simple NATS server does not provide at-least-once message delivery guarantees for publishing messages to topics.

This library is the result of my idea to unify the messaging patterns of both NATS and NATS Streaming into one universal client for both libraries.

## How make tests

    make test_env_up 
    make tests
    make test_env_down

## Examples 

See the examples directory for usage.

You can also check the unit tests.
