# Proof of Concept

Tiny project to prove a few things.

## Setup

1. Follow steps 1 and 2 in https://kafka.apache.org/quickstart.
2. Run `go run main.go`. This will start a server that is both listening for
message from the producer (via an HTTP call), and consuming the Kafka topic.

## Usage

Once Kafka and this little server are running, you can start writing:

    curl -X POST http://localhost:8000?key=somekey&value=somevalue

When you produce values, you'll see the consumer echo the content to STDOUT.

## Reference

Use https://github.com/segmentio/kafka-go as your Kafka client for now. It's not
the most mature client, but it's the most high level, so it's suitable for this
proof of concept.

## Things to prove / Questions to answer

- What are Kafka's write semantics?
- What are Kafka's read semantics?
- How are topics consumed? Are consumers stateful? Can logs be replayed?
- How would a "materializer" consume the log for the purposes of materializing?
