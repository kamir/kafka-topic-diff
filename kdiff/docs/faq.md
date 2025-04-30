# KDIFF Frequently Asked Questions

This document answers common questions about KDIFF and its usage.

## General Questions

### What is KDIFF?

KDIFF is a tool for comparing Kafka topics. It allows you to verify data consistency between topics, identify missing messages, detect content differences, and compare metadata like offsets, timestamps, and schema IDs.

### How is KDIFF different from other Kafka tools?

KDIFF specializes in topic comparison with features specifically designed for message-level validation:
- Supports both ordered (stream) and unordered (table) comparison strategies
- Provides detailed difference reports with multiple output formats
- Offers both CLI and service modes with REST API/WebUI
- Built-in support for validating migrations (MirrorMaker 2, Replicator, Cluster Linking)
- Optimized for large topics with streaming processing

### Does KDIFF require access to Kafka Schema Registry?

No, KDIFF works without Schema Registry. However, if you want to compare schema IDs, KDIFF can extract schema IDs from messages produced with Confluent's serializers and include them in the comparison.

### Can KDIFF compare topics across Kafka clusters with different security configurations?

Yes. KDIFF supports separate client configurations for each cluster, allowing you to compare topics across clusters with different security settings (PLAINTEXT, SSL, SASL).

## Installation & Configuration

### What are the system requirements for KDIFF?

- Python 3.8 or later
- Access to Kafka clusters you want to compare
- Network connectivity to Kafka brokers
- Sufficient permissions to read topics and create consumer groups

### How do I configure KDIFF to connect to secure Kafka clusters?

Create a client.properties file with your security configurations:

```properties
bootstrap.servers=kafka.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
```

Then add it to KDIFF:

```bash
kdiff config cluster --add /path/to/client.properties --name my-secure-cluster
```

### Where does KDIFF store its configuration?

By default, KDIFF stores configuration in the `~/ktools` directory:
- `~/ktools/kdiff.yaml`: Global settings
- `~/ktools/clusters.yaml`: Cluster configurations
- `~/ktools/clusters/`: Directory for client properties files

You can specify a different location with the `--config-dir` option.

## Usage Questions

### How do I compare topics with different names?

Specify different topic names for the source and target:

```bash
kdiff diff --cluster-a cluster1 --topic-a original-topic \
           --cluster-b cluster2 --topic-b renamed-topic
```

### How do I limit the number of messages compared?

Use the `--max-messages` option:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --max-messages 1000
```

### How do I compare messages from a specific time range?

Use the timestamp options (in milliseconds since epoch):

```bash
# Compare messages from last hour
HOUR_AGO=$(date -d "1 hour ago" +%s000)
NOW=$(date +%s000)

kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-timestamp-a $HOUR_AGO --end-timestamp-a $NOW \
  --start-timestamp-b $HOUR_AGO --end-timestamp-b $NOW
```

### What's the difference between stream and table comparison strategies?

- **Stream comparison**: Compares messages in sequence, position by position. Used when the order of messages matters.
- **Table comparison**: Compares messages as unordered collections, matching by key or content. Used when only the message content matters, not the order.

Choose based on your use case:
```bash
# Stream comparison (ordered)
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy stream

# Table comparison (unordered)
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy table
```

### How do I handle comparison of topics with large messages?

For topics with large messages, use checksum comparison for better performance:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --checksum-algorithm SHA256
```

### Can KDIFF compare specific partitions only?

Yes, you can specify partitions to compare:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --partitions-a 0,1 --partitions-b 0,1
```

### How do I integrate KDIFF into CI/CD pipelines?

KDIFF's exit codes make it easy to integrate into CI/CD pipelines:
- `0`: Success (no differences found)
- `1`: Error occurred
- `2`: Differences found

Example script:
```bash
#!/bin/bash
kdiff diff --cluster-a prod --topic-a orders --cluster-b staging --topic-b orders

if [ $? -eq 0 ]; then
  echo "Validation successful - topics match"
  exit 0
elif [ $? -eq 2 ]; then
  echo "Validation failed - topics differ"
  exit 1
else
  echo "Error during validation"
  exit 1
fi
```

## Performance Questions

### How does KDIFF handle very large topics?

KDIFF uses streaming processing to efficiently handle large topics:
- Messages are processed as they're read, not loaded entirely into memory
- Constant memory usage regardless of topic size
- Early termination after finding a specified number of differences
- Support for limiting comparison to specific ranges (offsets/timestamps)

### What is the performance impact of different comparison strategies?

- **Stream comparison** is faster as it simply compares messages in sequence
- **Table comparison** is slower due to the need to build hash tables for matching messages
- For very large topics, table comparison requires more memory

### How can I optimize KDIFF for better performance?

1. Limit the comparison scope:
   ```bash
   # Limit by message count
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --max-messages 10000
   
   # Limit by time range
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --start-timestamp-a 1609459200000 --end-timestamp-a 1609545600000
   ```

2. Use checksums for large messages:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --checksum-algorithm SHA256
   ```

3. Set a maximum difference limit:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --max-differences 10
   ```

## Troubleshooting

### KDIFF reports differences but the messages look identical

This could be due to invisible characters, whitespace, or binary differences. Use JSON output format to see detailed hex representations:

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --output-format json
```

### I'm getting "Topic not found" errors

1. Verify the topic exists:
   ```bash
   kdiff topic --cluster my-cluster --list | grep my-topic
   ```

2. Check that you have the correct permissions to access the topic
3. Ensure the topic name is correct (case-sensitive)

### How do I resolve "Failed to connect to cluster" errors?

1. Verify the bootstrap servers in your client properties file
2. Test network connectivity to the Kafka brokers
3. Ensure authentication credentials are correct
4. Check for SSL/TLS configuration issues

See the [Troubleshooting Guide](troubleshooting_guide.md) for more detailed steps.

## Service Mode Questions

### How do I run KDIFF in service mode?

```bash
# Start the service on the default port (8080)
kdiff service --start

# Specify a different port
kdiff service --start --port 9000
```

### How do I secure the KDIFF service API?

KDIFF service supports several authentication methods:
1. Basic Authentication:
   ```bash
   kdiff service --start --auth basic --username admin --password-file /path/to/password
   ```

2. JWT Authentication:
   ```bash
   kdiff service --start --auth jwt --jwt-secret-file /path/to/secret
   ```

3. API Key:
   ```bash
   kdiff service --start --auth apikey --apikey-file /path/to/keys
   ```

### Can I integrate KDIFF's API with other monitoring tools?

Yes, KDIFF's REST API allows integration with various monitoring and alerting tools:
- Use the `/comparisons` endpoint to start comparisons
- Implement webhooks to receive notifications when comparisons complete
- Use the JSON output format for machine-readable results

See the [REST API Reference](rest_api_reference.md) for detailed endpoint documentation.

## Migration Validation Questions

### How do I validate a MirrorMaker 2 migration?

```bash
# For MirrorMaker 2, use table strategy as message order might change
kdiff diff --cluster-a source-cluster --topic-a source-topic \
           --cluster-b target-cluster --topic-b source-topic \
           --strategy table --compare-content
```

### How do I validate a Confluent Replicator migration with topic renaming?

```bash
# For Replicator with topic renaming
kdiff diff --cluster-a source-cluster --topic-a original-topic \
           --cluster-b target-cluster --topic-b source.original-topic \
           --strategy table --compare-content
```

### How do I validate Confluent Cluster Linking migrations?

```bash
# For Cluster Linking, use stream strategy as offsets are preserved
kdiff diff --cluster-a source-cluster --topic-a source-topic \
           --cluster-b target-cluster --topic-b source-topic \
           --strategy stream --compare-content --compare-offsets
```

### What should I do if KDIFF reports differences in a migration validation?

1. Check the type of differences (content, offsets, timestamps, schema IDs)
2. Review specific messages that differ using the verbose output:
   ```bash
   kdiff diff --cluster-a source --topic-a source-topic \
              --cluster-b target --topic-b target-topic \
              --verbose
   ```
3. Look for patterns in the differences (specific fields, message types, etc.)
4. Investigate the migration configuration for potential issues
5. For content differences, consider using a more flexible comparison (e.g., ignoring certain fields)

See the [User Guide](user_guide.md) for more detailed migration validation procedures.
