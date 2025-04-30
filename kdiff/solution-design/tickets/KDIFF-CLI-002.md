# KDIFF-CLI-002: CLI Packaging and Distribution

## Description
Package the kdiff CLI tool for easy distribution and installation in Python environments, and create a command wrapper for simpler invocation.

## Requirements
- Create setuptools configuration for building a Python wheel package
- Implement package dependencies management
- Create entry points for direct command invocation
- Develop a shell script wrapper for simplified execution
- Ensure package works with standard Python package managers (pip, pipx)
- Include README and installation instructions
- Implement version management
- Support both development and production installations

## Definition of Done
- Package can be built successfully using standard Python tooling
- Wheel package installs correctly with pip
- Command-line entry point works after installation
- Shell wrapper script simplifies invocation
- Installation instructions are clear and accurate
- Version information is accessible through the CLI
- Installation works on multiple platforms (Linux, macOS)
- Package structure follows Python best practices

## Estimated Effort
2 days

## Dependencies
- KDIFF-CLI-001: Command Line Interface
