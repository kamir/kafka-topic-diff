# KDIFF-CORE-005: Table Comparison Strategy

## Description
Implement the Table Comparison Strategy that compares messages between two Kafka topics treating them as unordered collections where only content equality matters, not sequence.

## Requirements
- Create a `TableComparator` class that implements the table comparison strategy
- Compare message content between topics regardless of message order
- Support checksum/hash comparison for content verification
- Implement efficient matching algorithm for large message sets
- Identify messages that exist in one topic but not the other
- Identify duplicate messages within each topic
- Provide detailed reports of differences
- Support configurable key fields for matching messages across topics

## Definition of Done
- Two message collections can be compared with correct matching
- Messages unique to each topic are correctly identified
- Duplicate messages are correctly detected
- Comparison is efficient for large message sets
- Reports clearly identify different message patterns
- Unit tests verify correct comparison behavior
- Performance tests ensure algorithm scales with large datasets

## Estimated Effort
5 days

## Dependencies
- KDIFF-CORE-003: Topic Reader Implementation
