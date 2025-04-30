# KDIFF: Kafka Topic Comparison Tool

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/badge/pypi-v1.0.0-blue.svg)](https://pypi.org/project/kdiff/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/your-org/kdiff)
[![Coverage](https://img.shields.io/badge/coverage-81%25-yellowgreen.svg)](https://github.com/your-org/kdiff)

KDIFF is a powerful, flexible tool for comparing Kafka topics. Whether you're validating data migrations, ensuring data consistency, or troubleshooting message differences between environments, KDIFF provides the capabilities you need.

## Key Features

- **Comprehensive Comparison**: Compare message content, offsets, timestamps, and schema IDs between Kafka topics
- **Multiple Comparison Strategies**: 
  - **Stream strategy**: Compare messages in sequence (when order matters)
  - **Table strategy**: Compare messages as unordered collections (when only content matters)
- **Flexible Operation Modes**:
  - **CLI tool**: For quick comparisons and automation scripts
  - **Service mode**: With REST API and WebUI for continuous monitoring
- **Migration Validation**: Built-in support for validating topics migrated via MirrorMaker 2, Confluent Replicator, or Cluster Linking
- **Performance Optimized**: Streaming processing for efficient handling of very large topics
- **Detailed Reporting**: Comprehensive differences report with filtering and formatting options

## Installation

### From PyPI (Recommended)

```bash
pip install kdiff
```

### From Source

```bash
git clone https://github.com/your-org/kdiff.git
cd kdiff
pip install -e .
```

## Quick Start

### Initialize Configuration

```bash
# Create default configuration files
kdiff init
```

### Add Kafka Clusters

```bash
# Add a Kafka cluster configuration
kdiff config cluster --add /path/to/client.properties --name my-cluster
```

### Compare Topics

```bash
# Compare two topics on the same cluster
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2

# Compare topics on different clusters
kdiff diff --cluster-a cluster1 --cluster-b cluster2 --topic-a topic1 --topic-b topic2
```

### Validation Exit Codes

- `0`: Success (no differences found)
- `1`: Error occurred
- `2`: Differences found

This allows KDIFF to be used in CI/CD pipelines and automation scripts.

## Documentation

Comprehensive documentation is available in the [docs/](docs/README.md) directory:

- [User Guide](docs/user_guide.md)
- [Quick Start Guide](docs/quick_start_guide.md) 
- [Architecture Guide](docs/architecture_guide.md)
- [Developer Guide](docs/developer_guide.md)
- [Troubleshooting Guide](docs/troubleshooting_guide.md)
- [REST API Reference](docs/rest_api_reference.md)

## Use Cases

### Data Migration Validation

Verify the success of your Kafka topic migrations:

```bash
# Validate MirrorMaker 2 migration
kdiff diff --cluster-a source-cluster --cluster-b target-cluster \
  --topic-a source-topic --topic-b target-topic \
  --strategy table --compare-content
```

### Disaster Recovery Testing

Ensure your DR setup maintains data consistency:

```bash
# Compare primary and backup clusters
kdiff diff --cluster-a primary-cluster --cluster-b dr-cluster \
  --topic-a critical-data --topic-b critical-data \
  --strategy table --compare-content --compare-timestamps
```

### Cross-Environment Verification

Validate data consistency across environments:

```bash
# Compare production and staging
kdiff diff --cluster-a prod-cluster --cluster-b staging-cluster \
  --topic-a user-events --topic-b user-events \
  --strategy table --compare-content
```

## Examples

### Basic Comparison

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

### Table Strategy (Ignoring Order)

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 --strategy table
```

### Complete Metadata Comparison

```bash
kdiff diff --cluster-a cluster1 --cluster-b cluster2 --topic-a topic1 --topic-b topic2 \
  --compare-content --compare-offsets --compare-timestamps --compare-schema-ids
```

### Time-Based Comparison

```bash
# Compare messages from last hour
HOUR_AGO=$(date -d "1 hour ago" +%s000)
NOW=$(date +%s000)

kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --start-timestamp-a $HOUR_AGO --end-timestamp-a $NOW \
  --start-timestamp-b $HOUR_AGO --end-timestamp-b $NOW
```

### Generate JSON Report

```bash
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2 \
  --output-format json --output-file report.json
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The Apache Kafka community
- Confluent for their Kafka clients and tools
- All contributors to this project
