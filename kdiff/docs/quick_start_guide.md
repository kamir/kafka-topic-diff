# KDIFF Quick Start Guide

This guide provides a quick introduction to get you started with KDIFF for comparing Kafka topics.

## Installation

```bash
# Install from PyPI
pip install kdiff

# Verify installation
kdiff --version
```

## Initial Setup

```bash
# Initialize configuration directories and files
kdiff init

# This creates:
# - ~/ktools/kdiff.yaml
# - ~/ktools/clusters.yaml
# - ~/ktools/clusters/ directory
```

## Configure Kafka Clusters

1. Create a client properties file for your Kafka cluster:

```properties
# ~/ktools/clusters/my-cluster.properties
bootstrap.servers=kafka1.example.com:9092,kafka2.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";
```

2. Add the cluster to KDIFF:

```bash
kdiff config cluster --add ~/ktools/clusters/my-cluster.properties --name my-cluster
```

3. Verify the cluster was added:

```bash
kdiff config cluster --list
```

## Basic Topic Comparison

Compare two topics on the same cluster:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

The exit code indicates the result:
- `0`: Success (no differences)
- `1`: Error occurred
- `2`: Differences found

## Common Use Cases

### Compare Topics on Different Clusters

```bash
kdiff diff --cluster-a cluster1 --cluster-b cluster2 --topic-a topic1 --topic-b topic2
```

### Ignore Message Order (Table Strategy)

Use the table strategy when order doesn't matter, only content:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy table
```

### Compare Specific Time Range

Compare only messages from a specific time range:

```bash
# Time in milliseconds since epoch
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-timestamp-a 1609459200000 --end-timestamp-a 1609545600000 \
  --start-timestamp-b 1609459200000 --end-timestamp-b 1609545600000
```

### Limit Message Count

Only compare a specific number of messages:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --max-messages 1000
```

### Compare All Metadata

Compare content, offsets, timestamps, and schema IDs:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --compare-content --compare-offsets --compare-timestamps --compare-schema-ids
```

### Output JSON Format

Generate machine-readable JSON output:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --output-format json
```

### Save Report to File

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --output-file report.txt
```

## Working with Topics

### List Available Topics

```bash
kdiff topic --cluster my-cluster --list
```

### Read Messages from a Topic

```bash
kdiff topic --cluster my-cluster --read --topic my-topic --max-messages 10
```

### Count Messages

```bash
kdiff topic --cluster my-cluster --read --topic my-topic --count-only
```

## Migration Validation

Validate a topic migrated with MirrorMaker or Replicator:

```bash
kdiff diff --cluster-a source-cluster --cluster-b target-cluster \
  --topic-a source-topic --topic-b target-topic \
  --strategy table --compare-content
```

## Tips for Large Topics

1. Use offsets to limit the range:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-offset-a 1000 --end-offset-a 2000 \
  --start-offset-b 1000 --end-offset-b 2000
```

2. Use checksums for better performance with large messages:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --checksum-algorithm SHA256
```

3. Sample a percentage of messages:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --sample-rate 0.1  # Compare 10% of messages
```

## Automation Example

```bash
#!/bin/bash
# Script to validate topic migration

kdiff diff --cluster-a source --topic-a source-topic \
           --cluster-b target --topic-b target-topic \
           --strategy table --compare-content

if [ $? -eq 0 ]; then
  echo "Migration successful - topics are identical"
  exit 0
elif [ $? -eq 2 ]; then
  echo "Migration failed - differences found"
  exit 1
else
  echo "Error during validation"
  exit 1
fi
```

## Getting Help

```bash
# General help
kdiff --help

# Command-specific help
kdiff diff --help
kdiff topic --help
kdiff config --help
```

For more detailed information, refer to the [User Guide](./user_guide.md).
