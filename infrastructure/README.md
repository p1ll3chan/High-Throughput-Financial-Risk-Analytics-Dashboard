# Local Kafka infrastructure

This Compose project runs one Kafka broker in KRaft mode and creates the
`market-ticks` topic after the broker becomes healthy.

## Start

From the repository root:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
docker compose -f infrastructure/docker-compose.yml ps
```

Kafka is available to applications running on the host at `localhost:9092`.
The topic initializer exits successfully after it creates (or confirms) the
`market-ticks` topic.

## Verify

First confirm that the one-time topic initializer completed successfully:

```bash
docker compose -f infrastructure/docker-compose.yml ps --all
docker compose -f infrastructure/docker-compose.yml logs kafka-topic-init
```

`kafka-topic-init` should show `Exited (0)` after it has printed the topic
description. It can appear as `Up` briefly while it is creating the topic.

Then verify the topic metadata:

```bash
docker compose -f infrastructure/docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --describe --topic market-ticks
```

Expected output includes `Topic: market-ticks`, `PartitionCount:3`, and
`ReplicationFactor:1`. Override the partition count before the first startup
if needed:

```bash
KAFKA_MARKET_TICKS_PARTITIONS=6 docker compose -f infrastructure/docker-compose.yml up -d
```

### Optional end-to-end smoke test

This confirms that Kafka can deliver a new record through `market-ticks`.
In one terminal, start a consumer with a new group:

```bash
docker compose -f infrastructure/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic market-ticks \
  --group "kafka-verification-$(date +%s)" \
  --property print.key=true \
  --property key.separator=:
```

In a second terminal, publish a test record:

```bash
printf 'verification:phase-1-ok\n' | \
  docker compose -f infrastructure/docker-compose.yml exec -T kafka \
    kafka-console-producer --bootstrap-server localhost:9092 \
    --topic market-ticks \
    --property parse.key=true \
    --property key.separator=:
```

The consumer should print `verification:phase-1-ok`. Press `Ctrl+C` to stop
it. This test intentionally leaves one clearly labelled record in the topic.

## Stop

```bash
docker compose -f infrastructure/docker-compose.yml down
```

To also remove the persisted Kafka data volume:

```bash
docker compose -f infrastructure/docker-compose.yml down -v
```
