# KDIFF-SVC-005: WebUI - Diff Configuration

## Description
Implement the Diff Configuration panel in the WebUI that allows users to set up the parameters for the diff operation between two Kafka topics.

## Requirements
- Create input fields for start/end offsets for both topics
- Implement timestamp selection with date/time pickers
- Add input for maximum messages to compare
- Create checkboxes for comparison options (content, offsets, timestamps, schema IDs)
- Implement radio buttons for strategy selection (stream vs. table)
- Add dropdown for checksum algorithm selection
- Create "Run Diff" action button
- Implement configuration presets/templates
- Add form validation for inputs

## Definition of Done
- All configuration parameters can be set through the UI
- Validation prevents invalid configurations
- Configuration changes update the state appropriately
- Form allows both offset and timestamp-based configuration
- Strategy selection correctly toggles relevant options
- Run button triggers the diff operation with correct parameters
- Configuration can be saved as presets for future use
- UI provides helpful tooltips for configuration options
- UI is responsive across different screen sizes

## Estimated Effort
3 days

## Dependencies
- KDIFF-SVC-003: WebUI - Core Framework
- KDIFF-SVC-004: WebUI - Cluster/Topic Selection
