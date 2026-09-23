# rmqtail

A standalone CLI to connect to a RabbitMQ cluster and tail messages from an exchange or queue in real-time. Fills the gap left by Python-based `rabbitmqadmin` or heavy web UIs.

## Status

**built, untested**: the AMQP client logic and message streaming is built. It has not been run against a real RabbitMQ instance in this sandbox environment.

## Installation

```sh
cargo install --path .
```

## Usage

```sh
# Tail an existing queue
rmqtail --queue my-queue amqp://guest:guest@127.0.0.1:5672/%2f

# Bind a temporary queue to an exchange and tail it
rmqtail --exchange my-exchange --routing-key "logs.#"
```
