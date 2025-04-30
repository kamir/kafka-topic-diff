# KDIFF-CORE-004: Stream Comparison Strategy

## Description
Implement the Stream Comparison Strategy that compares messages between two Kafka topics treating them as ordered streams where sequence matters.

## Requirements
- Create a `StreamComparator` class that implements the stream comparison strategy
- Compare message content using binary-level comparison
- Support checksum/hash comparison for content verification
- Optionally compare message offsets
- Optionally compare message timestamps
- Optionally compare schema IDs
- Ensure strict sequence matching (A1 corresponds to B1, A2 to B2, etc.)
- Identify and report any discrepancies found
- Provide detailed information about mismatches

## Definition of Done
- Two message streams can be compared with correct matching
- Content differences are correctly identified
- Offset/timestamp/schema ID comparisons work as expected
- Comparison is efficient for large message sets
- Reports provide clear information about found discrepancies
- Unit tests verify correct comparison behavior
- Edge cases (empty streams, single message, etc.) are handled correctly

## Estimated Effort
4 days

## Dependencies
- KDIFF-CORE-003: Topic Reader Implementation
