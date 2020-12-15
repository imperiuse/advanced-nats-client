# Advance Nats Streaming

![img](https://img.shields.io/badge/GPL%20v3.0-GNU%20GENERAL%20PUBLIC%20LICENSE-blue) ![img](https://img.shields.io/badge/code--coverage-70%25-green)
[![Actions Status](https://github.com/imperiuse/advance-nats-client/workflows/Test/badge.svg)](https://github.com/imperiuse/advance-nats-client/actions)

Advance NATS and NATS-Streaming client library based on nats.go and stun.go projects

I try to bring together all pluses and minimize minuses NATS and NATS streaming.

For example:
As you know, Nats Streaming does not support Request-Reply semantic and simple NATS server does not support at least one guarantee publish msg to topics.

This library is a result, my idea put together NATS and NATS Streaming message pattern in one universal client for both library.

## How make tests

    make test_env_up 
    make tests
    make test_env_down

## Examples 

See in `examples` dir

Also, you can see Unit tests 
