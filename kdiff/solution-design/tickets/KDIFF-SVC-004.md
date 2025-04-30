# KDIFF-SVC-004: WebUI - Cluster/Topic Selection

## Description
Implement the Cluster and Topic Selection panel in the WebUI that allows users to browse and select Kafka clusters and topics for comparison.

## Requirements
- Create dropdown selectors for Cluster A and Cluster B
- Implement Topic browser for each selected cluster
- Add search functionality for finding topics in large clusters
- Display topic metadata (partitions, size, etc.) when available
- Implement topic favorites for quick access
- Add recently used topics list
- Create visual indication of topic status/health
- Implement loading states for async operations
- Ensure accessibility for keyboard navigation

## Definition of Done
- Users can select clusters from dropdown menus
- Topic lists load correctly for selected clusters
- Topics can be searched and filtered
- Selected topics are correctly passed to the diff configuration
- Topic metadata is displayed when available
- UI provides clear feedback during loading states
- Keyboard navigation works for all selection components
- Components are responsive across different screen sizes
- Unit tests verify selection behavior

## Estimated Effort
3 days

## Dependencies
- KDIFF-SVC-003: WebUI - Core Framework
