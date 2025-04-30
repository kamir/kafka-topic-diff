# KDIFF User Guide

## Overview

KDIFF (Kafka Diff) is a tool for comparing messages between Kafka topics. It can help you:

- Verify data consistency between two Kafka topics
- Identify missing messages in either topic
- Detect content differences between corresponding messages
- Compare metadata like offsets, timestamps, and schema IDs
- Validate successful migration of topics across clusters

KDIFF supports two comparison modes:
- **Stream comparison**: Compares messages in sequence, useful when order matters
- **Table comparison**: Compares messages as unordered collections, useful when only content matters

## Installation

### Prerequisites

- Python 3.8 or later
- Access to Kafka clusters you want to compare

### Installing from PyPI

```bash
pip install kdiff
```

### Installing from Source

```bash
git clone https://github.com/your-org/kdiff.git
cd kdiff
pip install -e .
```

## Configuration

KDIFF uses a configuration directory structure to manage Kafka cluster configurations. By default, this is located in `~/ktools`.

### Initializing Configuration

You can initialize the default configuration files with:

```bash
kdiff init
```

This creates:
- `~/ktools/kdiff.yaml`: Global settings for KDIFF
- `~/ktools/clusters.yaml`: Cluster configurations
- `~/ktools/clusters/`: Directory for Kafka client properties files

### Configuring Clusters

To add a Kafka cluster to KDIFF:

```bash
kdiff config cluster --add /path/to/client.properties --name my-cluster
```

You can list configured clusters with:

```bash
kdiff config cluster --list
```

And remove a cluster with:

```bash
kdiff config cluster --remove my-cluster
```

#### Example client.properties

```properties
bootstrap.servers=kafka1.example.com:9092,kafka2.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
```

### Global Configuration (kdiff.yaml)

The `kdiff.yaml` file controls default behavior:

```yaml
default:
  checksum_algorithm: "SHA256"
  output_format: "text"
  strategy: "stream"
  comparison:
    content: true
    offsets: false
    timestamps: false
    schema_ids: false
  max_messages: 1000
```

## Basic Usage

### Comparing Topics

To compare two topics on the same cluster:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

To compare topics on different clusters:

```bash
kdiff diff --cluster-a cluster1 --cluster-b cluster2 --topic-a topic1 --topic-b topic2
```

### Limiting the Comparison Range

You can limit the comparison range using offsets:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-offset-a 1000 --end-offset-a 2000 \
  --start-offset-b 1000 --end-offset-b 2000
```

Or using timestamps (Unix time in milliseconds):

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-timestamp-a 1609459200000 --start-timestamp-b 1609459200000
```

You can also limit the number of messages to compare:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --max-messages 10000
```

### Choosing Comparison Strategy

For ordered comparison (stream mode):

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy stream
```

For unordered comparison (table mode):

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy table
```

### Selecting Comparison Criteria

By default, KDIFF compares message content. You can enable other comparisons:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --compare-content --compare-timestamps --compare-schema-ids
```

### Output Formats

For human-readable text output:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --output-format text
```

For machine-readable JSON output:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --output-format json
```

## Working with Topics

### Listing Topics

To list available topics on a cluster:

```bash
kdiff topic --cluster my-cluster --list
```

### Reading Messages

To read messages from a topic:

```bash
kdiff topic --cluster my-cluster --read --topic my-topic
```

You can specify ranges and formats:

```bash
kdiff topic --cluster my-cluster --read --topic my-topic \
  --start-offset 1000 --max-messages 10 --format json
```

## Validating Kafka Topic Migrations

KDIFF is an excellent tool for validating data consistency after migrating topics between Kafka clusters using MirrorMaker 2, Confluent Replicator, or Confluent Cluster Linking.

### MirrorMaker 2 Validation

When validating MirrorMaker 2 migrations, follow these steps:

1. **Verify topic configuration**:
   First, check that topic configurations are properly mirrored:

   ```bash
   # Use Kafka AdminClient tools to verify configurations
   kafka-topics.sh --describe --bootstrap-server source-cluster:9092 --topic source-topic
   kafka-topics.sh --describe --bootstrap-server target-cluster:9092 --topic source-topic
   ```

2. **Validate message content**:
   Use KDIFF to compare message content between source and target clusters:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b source-topic \
             --strategy table --compare-content
   ```

   The table strategy is recommended as MirrorMaker 2 preserves message content but not necessarily the exact ordering.

3. **Check for missing messages**:
   Look at the summary in the KDIFF output to find any missing messages:

   ```
   Summary:
     Messages compared: 10000
     Differences found: 0
     Missing in source: 0
     Missing in target: 0
   ```

4. **Verify metadata (optional)**:
   If you need to validate that metadata is properly preserved:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b source-topic \
             --compare-content --compare-timestamps --compare-schema-ids
   ```

### Confluent Replicator Validation

Confluent Replicator may include topic name transformations. When validating:

1. **Identify the target topic name**:
   Replicator often prefixes topic names, e.g., `source-topic` might become `source.source-topic`:

   ```bash
   kdiff topic --cluster target-cluster --list | grep source-topic
   ```

2. **Compare message content**:
   Accounting for the topic name change:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b source.source-topic \
             --strategy table --compare-content
   ```

3. **Validate completeness**:
   Ensure no messages are lost during replication:

   ```bash
   # Get message counts from both topics
   kdiff topic --cluster source-cluster --read --topic source-topic --count-only
   kdiff topic --cluster target-cluster --read --topic source.source-topic --count-only
   ```

4. **Continuous validation**:
   For ongoing replication, use timestamps to validate only recent data:

   ```bash
   # Get current time in milliseconds
   CURRENT_TIME=$(date +%s000)
   # Check last hour (3600000 ms)
   START_TIME=$(expr $CURRENT_TIME - 3600000)
   
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b source.source-topic \
             --start-timestamp-a $START_TIME --start-timestamp-b $START_TIME \
             --strategy table --compare-content
   ```

### Confluent Cluster Linking Validation

Cluster Linking provides real-time mirroring with preserved offsets. To validate:

1. **Verify topic status**:
   Check that the mirror topic is active and synced:

   ```bash
   # Using Confluent CLI
   confluent kafka mirror describe --source-cluster source-cluster --link my-link --topic source-topic
   ```

2. **Compare with offset preservation**:
   Since Cluster Linking preserves offsets, use the stream strategy:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b source-topic \
             --strategy stream --compare-content --compare-offsets
   ```

3. **Validate consumer position**:
   Ensure consumer offsets can be properly translated:

   ```bash
   # Get consumer group offsets from source cluster
   kafka-consumer-groups.sh --bootstrap-server source-cluster:9092 --group test-group --describe
   
   # Verify equivalent position in target cluster
   kafka-consumer-groups.sh --bootstrap-server target-cluster:9092 --group test-group --describe
   ```

4. **Monitor replication lag**:
   For ongoing monitoring, track replication lag:

   ```bash
   # Using Confluent CLI
   confluent kafka mirror status --source-cluster source-cluster --link my-link
   ```

   A zero or very low lag value indicates successful mirroring.

### Best Practices for Migration Validation

1. **Test with sample data first**:
   Before validating a full production migration, test with a subset:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b target-topic \
             --max-messages 1000 --strategy table --compare-content
   ```

2. **Verify before cutover**:
   Always validate thoroughly before redirecting clients to the new cluster.

3. **Staged validation**:
   For large topics, validate in stages:
   - First, check message counts match
   - Then sample random messages throughout the topic
   - Finally, run detailed comparisons on specific time ranges

4. **Use checksums for large messages**:
   When dealing with very large messages, use checksum comparison for efficiency:

   ```bash
   kdiff diff --cluster-a source-cluster --topic-a source-topic \
             --cluster-b target-cluster --topic-b target-topic \
             --checksum-algorithm SHA256 --strategy table
   ```

5. **Automate validation in scripts**:
   Create validation scripts that use KDIFF's exit codes:
   - Exit code 0: Success (no differences)
   - Exit code 2: Differences found

   ```bash
   #!/bin/bash
   kdiff diff --cluster-a source --topic-a source-topic --cluster-b target --topic-b target-topic
   
   if [ $? -eq 0 ]; then
     echo "Migration successful!"
   elif [ $? -eq 2 ]; then
     echo "Migration failed: data differences detected"
     exit 1
   else
     echo "Error during validation"
     exit 1
   fi
   ```

## Exit Codes

KDIFF uses the following exit codes:

- `0`: Success (no differences found)
- `1`: Error occurred
- `2`: Differences found

This allows you to use KDIFF in scripts and CI pipelines to check for topic equality.

## Troubleshooting

### Common Issues

#### Connection Problems

If you encounter connection issues:

1. Verify your client properties file has the correct bootstrap servers
2. Check network connectivity to Kafka brokers
3. Verify authentication credentials if using security

#### Performance Considerations

For comparing large topics:

1. Use offsets or timestamps to limit the comparison range
2. Consider using the table comparison strategy if order doesn't matter
3. Use `--max-messages` to limit the number of messages compared during initial testing

#### Checksum Mismatch but Content Seems Identical

If you're seeing content differences but the messages appear identical:

1. Use the `--output-format json` option to see detailed hex representations
2. Check for whitespace differences
3. Verify the message has the exact same bytes (binary differences may not be visible in text)

### Getting Help

For more help on any command:

```bash
kdiff --help
kdiff diff --help
kdiff topic --help
kdiff config --help
```

## Quick Start Guide

Here's a quick reference for common scenarios:

### Simple Topic Comparison
```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

### Cross-Cluster Topic Comparison
```bash
kdiff diff --cluster-a cluster1 --cluster-b cluster2 --topic-a topic1 --topic-b topic1
```

### Content-Only Comparison (Ignoring Order)
```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy table --compare-content
```

### Complete Metadata Comparison
```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --compare-content --compare-offsets --compare-timestamps --compare-schema-ids
```

### Migration Validation
```bash
kdiff diff --cluster-a source-cluster --cluster-b target-cluster \
  --topic-a source-topic --topic-b target-topic --strategy table --compare-content
