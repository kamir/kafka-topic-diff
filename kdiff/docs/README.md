# KDIFF Documentation

Welcome to the KDIFF documentation. This repository contains comprehensive guides and references for using and understanding the KDIFF tool.

## Documentation Index

### User Documentation

* [**User Guide**](user_guide.md) - Comprehensive guide to using KDIFF, including installation, configuration, and all features
* [**Quick Start Guide**](quick_start_guide.md) - Get up and running with KDIFF in just a few minutes
* [**Troubleshooting Guide**](troubleshooting_guide.md) - Solutions for common issues and error scenarios
* [**FAQ**](faq.md) - Answers to frequently asked questions about KDIFF
* [**REST API Reference**](rest_api_reference.md) - Complete reference for the KDIFF REST API when running in service mode

### Developer Documentation

* [**Architecture Guide**](architecture_guide.md) - Detailed overview of the KDIFF architecture and design
* [**Developer Guide**](developer_guide.md) - Guide for developers who want to contribute to or extend KDIFF

## Key Features of KDIFF

- **Topic Comparison** - Compare message content and metadata between two Kafka topics
- **Multiple Comparison Strategies** - Stream comparison (ordered) and table comparison (unordered)
- **Flexible Criteria** - Compare message content, offsets, timestamps, schema IDs
- **CLI and Service Modes** - Use as a command-line tool or REST service with WebUI
- **Migration Validation** - Verify the success of topic migrations via MirrorMaker, Replicator, or Cluster Linking

## Getting Started

If you're new to KDIFF, we recommend starting with the [Quick Start Guide](quick_start_guide.md).

For a complete understanding of all features, refer to the [User Guide](user_guide.md).

## Installation

```bash
# Install from PyPI
pip install kdiff

# Verify installation
kdiff --version
```

## Basic Usage

```bash
# Initialize configuration
kdiff init

# Compare two topics
kdiff diff --cluster-a my-cluster --topic-a topic1 --topic-b topic2
```

## Support and Community

If you encounter issues not covered in the documentation:

1. Check the [Troubleshooting Guide](troubleshooting_guide.md)
2. Search for existing issues in the GitHub repository
3. Submit a new issue with detailed information about your problem

## Contributing

Contributions to KDIFF are welcome! See the [Developer Guide](developer_guide.md) for details on setting up a development environment and the contribution process.
