# KDIFF Troubleshooting Guide

This guide helps you diagnose and resolve common issues when using KDIFF for Kafka topic comparison.

## Common Error Messages

### Connection Issues

#### Error: Failed to connect to cluster

```
Error: Failed to connect to cluster my-cluster: Failed to connect to broker
```

**Possible Causes**:
1. Incorrect bootstrap servers
2. Network connectivity issues
3. Authentication failure
4. SSL/TLS configuration problems

**Solutions**:
1. Verify bootstrap servers in your client.properties file:
   ```bash
   cat ~/ktools/clusters/my-cluster.properties
   ```

2. Test network connectivity:
   ```bash
   ping kafka1.example.com
   telnet kafka1.example.com 9092
   ```

3. Check authentication credentials:
   ```bash
   # Verify SASL credentials
   grep "sasl.jaas.config" ~/ktools/clusters/my-cluster.properties
   ```

4. Verify SSL/TLS configuration:
   ```bash
   # If using SSL, ensure truststore and keystore paths exist
   grep "ssl.truststore.location" ~/ktools/clusters/my-cluster.properties
   ```

#### Error: Topic authorization failed

```
Error: Failed to read topic: Topic authorization failed for topic my-topic
```

**Solutions**:
1. Verify your user has read permissions for the topic
2. Check ACLs on the Kafka cluster:
   ```bash
   kafka-acls.sh --bootstrap-server kafka1.example.com:9092 \
     --command-config ~/ktools/clusters/my-cluster.properties \
     --list --topic my-topic
   ```

### Topic Issues

#### Error: Topic not found

```
Error: Topic my-topic not found
```

**Solutions**:
1. Check if the topic exists:
   ```bash
   kdiff topic --cluster my-cluster --list | grep my-topic
   ```

2. Verify topic name (case-sensitive)

3. Check if you have permissions to see the topic

#### Error: No messages found in topic

```
Warning: No messages found in topic my-topic
```

**Solutions**:
1. Verify the topic contains messages:
   ```bash
   kafka-console-consumer.sh --bootstrap-server kafka1.example.com:9092 \
     --topic my-topic --from-beginning --max-messages 1
   ```

2. Check if you're using the right time range:
   ```bash
   # To read the latest 10 messages
   kdiff topic --cluster my-cluster --read --topic my-topic --max-messages 10
   ```

3. Verify you have the right offsets:
   ```bash
   # Get topic info
   kafka-topics.sh --bootstrap-server kafka1.example.com:9092 \
     --describe --topic my-topic
   ```

### Configuration Issues

#### Error: Could not parse configuration

```
Error: Could not parse configuration file: ~/ktools/kdiff.yaml
```

**Solutions**:
1. Check YAML syntax:
   ```bash
   python -c "import yaml; yaml.safe_load(open('~/ktools/kdiff.yaml'))"
   ```

2. Reinitialize configuration:
   ```bash
   # Backup existing config
   mv ~/ktools ~/ktools_backup
   
   # Create new default config
   kdiff init
   ```

#### Error: Cluster configuration not found

```
Error: Cluster configuration not found for: my-cluster
```

**Solutions**:
1. Check available clusters:
   ```bash
   kdiff config cluster --list
   ```

2. Add the cluster:
   ```bash
   kdiff config cluster --add /path/to/client.properties --name my-cluster
   ```

3. Check clusters.yaml file:
   ```bash
   cat ~/ktools/clusters.yaml
   ```

## Performance Issues

### Slow Comparison for Large Topics

**Symptoms**:
- Comparison takes a very long time
- High memory usage
- System becomes unresponsive

**Solutions**:
1. Limit message count:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --max-messages 10000
   ```

2. Use time windows for incremental comparison:
   ```bash
   # Compare last day's messages
   YESTERDAY=$(date -d "yesterday" +%s000)
   TODAY=$(date +%s000)
   
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --start-timestamp-a $YESTERDAY --end-timestamp-a $TODAY \
     --start-timestamp-b $YESTERDAY --end-timestamp-b $TODAY
   ```

3. Use checksums for large messages:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --checksum-algorithm SHA256
   ```

4. Use the table strategy for faster unordered comparison:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --strategy table
   ```

## Comparison Result Issues

### False Differences

**Symptoms**:
- Topics appear different but should be identical
- Content differences show minor variations

**Solutions**:
1. Check for whitespace/formatting differences:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --output-format json --max-messages 5
   ```

2. Verify timestamps are within tolerance:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --compare-timestamps --timestamp-tolerance 5000
   ```

3. Try different comparison strategies:
   ```bash
   # If using stream comparison, try table
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --strategy table
   ```

### Missing Messages

**Symptoms**:
- Some messages found in one topic but not the other
- Inconsistent counts between topics

**Solutions**:
1. Verify message counts:
   ```bash
   kdiff topic --cluster my-cluster --read --topic topic1 --count-only
   kdiff topic --cluster my-cluster --read --topic topic2 --count-only
   ```

2. Check for message key differences:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --strategy table --compare-content --verbose
   ```

3. Examine specific offsets:
   ```bash
   # Check specific messages at known offsets
   kdiff topic --cluster my-cluster --read --topic topic1 \
     --start-offset 1000 --max-messages 1
   ```

## Schema-Related Issues

### Schema ID Differences

```
Differences: Schema IDs do not match (1 vs 2)
```

**Solutions**:
1. Verify schema compatibility:
   ```bash
   # If using Schema Registry
   curl -X GET http://schema-registry:8081/subjects/topic1-value/versions/latest
   curl -X GET http://schema-registry:8081/subjects/topic2-value/versions/latest
   ```

2. Ignore schema ID differences if only checking content:
   ```bash
   kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
     --compare-content --no-compare-schema-ids
   ```

### Content Deserialization Issues

```
Warning: Failed to deserialize message content at offset 123
```

**Solutions**:
1. Check message format:
   ```bash
   # Read raw bytes
   kdiff topic --cluster my-cluster --read --topic topic1 \
     --start-offset 123 --max-messages 1 --output-format json
   ```

2. Verify Schema Registry connection if using Avro/Protobuf:
   ```bash
   # If using Schema Registry
   curl -X GET http://schema-registry:8081/subjects
   ```

## Environment Issues

### Permissions Problems

```
Error: Permission denied: '/home/user/ktools/clusters/my-cluster.properties'
```

**Solutions**:
1. Check file permissions:
   ```bash
   ls -la ~/ktools/clusters/
   ```

2. Fix permissions:
   ```bash
   chmod 600 ~/ktools/clusters/my-cluster.properties
   ```

### Python Environment Issues

```
ImportError: No module named confluent_kafka
```

**Solutions**:
1. Verify installation:
   ```bash
   pip list | grep confluent-kafka
   ```

2. Reinstall with dependencies:
   ```bash
   pip install --upgrade kdiff[all]
   ```

3. Check for Python version compatibility:
   ```bash
   python --version
   # Should be 3.8 or later
   ```

## Advanced Troubleshooting

### Enabling Debug Logging

For detailed diagnostics:

```bash
# Enable debug logging
export KDIFF_LOG_LEVEL=DEBUG

# Run command with verbose output
kdiff --verbose diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

### Creating a Diagnostic Bundle

For submitting issues:

```bash
# Create diagnostic bundle (excluding sensitive data)
mkdir -p ~/kdiff-diagnostics
cp ~/ktools/kdiff.yaml ~/kdiff-diagnostics/
# Remove credentials from client.properties
grep -v "password" ~/ktools/clusters/my-cluster.properties > ~/kdiff-diagnostics/my-cluster-sanitized.properties
# Export logs
export KDIFF_LOG_FILE=~/kdiff-diagnostics/kdiff.log
kdiff --verbose diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
# Compress the bundle
tar -czvf kdiff-diagnostics.tar.gz ~/kdiff-diagnostics/
```

### Testing with Minimal Configuration

To isolate issues:

```bash
# Create a minimal test directory
mkdir -p ~/kdiff-test/clusters

# Create minimal config files
cat > ~/kdiff-test/kdiff.yaml << EOF
default:
  strategy: "stream"
  output_format: "text"
EOF

cat > ~/kdiff-test/clusters.yaml << EOF
clusters:
  test-cluster:
    config_file: "test-cluster.properties"
EOF

cat > ~/kdiff-test/clusters/test-cluster.properties << EOF
bootstrap.servers=localhost:9092
EOF

# Run with test config
kdiff --config-dir ~/kdiff-test topic --cluster test-cluster --list
```

## Kafka Migration Specific Issues

### MirrorMaker 2 Troubleshooting

**Problem**: Topics don't show up in target cluster

**Solutions**:
1. Check MirrorMaker configuration:
   ```bash
   # Verify MirrorMaker is running
   ps aux | grep MirrorMaker
   
   # Check connector status
   curl -s http://mirrormaker:8083/connectors/mirror-source-connector/status
   ```

2. Verify topic naming:
   ```bash
   # Topics might be renamed with prefixes
   kdiff topic --cluster target-cluster --list | grep -i source
   ```

### Confluent Replicator Issues

**Problem**: Differences in message content

**Solutions**:
1. Check for schema translation issues:
   ```bash
   # Check if Replicator is using Schema Translation
   curl -s http://replicator:8083/connectors/replicator-source/config | grep schema
   ```

2. Verify topic configuration:
   ```bash
   # Compare topic configurations
   kafka-topics.sh --bootstrap-server source-cluster:9092 --describe --topic source-topic
   kafka-topics.sh --bootstrap-server target-cluster:9092 --describe --topic source.source-topic
   ```

### Cluster Linking Troubleshooting

**Problem**: High replication lag

**Solutions**:
1. Check link status:
   ```bash
   # Using Confluent CLI
   confluent kafka mirror status --source-cluster source-cluster --link my-link
   ```

2. Verify consumer group offsets:
   ```bash
   # Check consumer group lag on source
   kafka-consumer-groups.sh --bootstrap-server source-cluster:9092 --group my-group --describe
   
   # Check on target
   kafka-consumer-groups.sh --bootstrap-server target-cluster:9092 --group my-group --describe
   ```

## Getting More Help

If you still encounter issues:

1. Check KDIFF GitHub repository for known issues
2. Search StackOverflow with the `kafka-topic-comparison` tag
3. Post detailed issue reports including:
   - KDIFF version (`kdiff --version`)
   - Command run (with sensitive data removed)
   - Error messages
   - Kafka cluster versions
   - Client configuration (with credentials removed)
