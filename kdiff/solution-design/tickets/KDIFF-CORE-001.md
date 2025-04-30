# KDIFF-CORE-001: Core Configuration Module

## Description
Implement the core configuration module that handles loading and managing configuration from the `~/ktools` directory.

## Requirements
- Create a `KDiffConfig` class that loads configuration from YAML files
- Support loading cluster configuration from `~/ktools/clusters/` directory
- Load global configuration from `~/ktools/kdiff.yaml`
- Load cluster aliases from `~/ktools/clusters.yaml`
- Provide defaults for all configuration values
- Implement environment variable overrides for critical settings
- Ensure configuration is validated during loading

## Definition of Done
- Configuration can be loaded from YAML files
- Default values are provided for missing configuration
- Cluster configurations are correctly resolved from aliases
- Unit tests verify correct loading and validation
- Configuration can be accessed through a clean API
- Environment variable overrides are documented

## Estimated Effort
2 days

## Dependencies
None
