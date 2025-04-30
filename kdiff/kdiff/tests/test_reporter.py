"""
Unit tests for the KDIFF reporter.
"""

import unittest
import json
import time
from unittest.mock import MagicMock, patch, call, ANY, mock_open
from typing import Dict, List, Iterator, Any

from kdiff.core.reader import KafkaMessage, MessageMeta
from kdiff.core.comparator import (
    ComparisonResult, MessageDifference, ComparisonOptions, 
    ChecksumAlgorithm
)
from kdiff.core.reporter import (
    DiffReporter, ReportOptions, ReportFormat, DiffSeverity
)


def create_test_message(content: bytes, offset: int, timestamp: int, partition: int = 0, 
                       key: bytes = None, schema_id: int = None) -> KafkaMessage:
    """Helper to create a test message."""
    meta = MessageMeta(
        offset=offset,
        timestamp=timestamp,
        partition=partition,
        key=key,
        schema_id=schema_id
    )
    return KafkaMessage(content=content, meta=meta)


def create_test_difference(index: int, difference_type: str, 
                          is_missing: bool = False, details: str = "Test difference",
                          message_a: KafkaMessage = None, message_b: KafkaMessage = None) -> MessageDifference:
    """Helper to create a test difference."""
    return MessageDifference(
        index=index,
        topic_a="topic-a",
        topic_b="topic-b",
        message_a=message_a,
        message_b=message_b,
        difference_type=difference_type,
        details=details
    )


def create_test_result(messages_compared: int = 100, 
                      differences_found: int = 5,
                      missing_in_a: int = 1,
                      missing_in_b: int = 2,
                      content_differences: int = 2,
                      offset_differences: int = 0,
                      timestamp_differences: int = 0,
                      schema_id_differences: int = 0,
                      differences: List[MessageDifference] = None) -> ComparisonResult:
    """Helper to create a test comparison result."""
    if differences is None:
        differences = []
        
    return ComparisonResult(
        topic_a="topic-a",
        topic_b="topic-b",
        messages_compared=messages_compared,
        differences_found=differences_found,
        missing_in_a=missing_in_a,
        missing_in_b=missing_in_b,
        content_differences=content_differences,
        offset_differences=offset_differences,
        timestamp_differences=timestamp_differences,
        schema_id_differences=schema_id_differences,
        differences=differences,
        start_time=time.time() - 10,
        end_time=time.time(),
        options=ComparisonOptions()
    )


class TestDiffReporter(unittest.TestCase):
    """Tests for the DiffReporter class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create default report options
        self.options = ReportOptions(
            format=ReportFormat.TEXT,
            verbosity=1,
            max_diff_details=None,
            include_content_diffs=True,
            include_offset_diffs=True,
            include_timestamp_diffs=True,
            include_schema_id_diffs=True,
            include_missing_messages=True,
            severity_threshold=DiffSeverity.INFO
        )
        
        # Create reporter
        self.reporter = DiffReporter(options=self.options)
        
        # Create test messages
        self.message_a1 = create_test_message(b"test1", 0, 1000)
        self.message_a2 = create_test_message(b"test2", 1, 2000)
        self.message_b1 = create_test_message(b"test1", 0, 1000)
        self.message_b2 = create_test_message(b"different", 1, 2000)
        
        # Create test differences
        self.content_diff = create_test_difference(
            index=1,
            difference_type="content",
            details="Content mismatch: 5 bytes vs 9 bytes",
            message_a=self.message_a2,
            message_b=self.message_b2
        )
        
        self.missing_diff = create_test_difference(
            index=2,
            difference_type="missing",
            is_missing=True,
            details="Message missing in topic-b",
            message_a=self.message_a1,
            message_b=None
        )
        
        self.timestamp_diff = create_test_difference(
            index=3,
            difference_type="timestamp",
            details="Timestamp mismatch: 1000 != 2000",
            message_a=self.message_a1,
            message_b=self.message_b2
        )
        
        # Create test result with differences
        self.differences = [self.content_diff, self.missing_diff, self.timestamp_diff]
        self.result = create_test_result(
            messages_compared=100,
            differences_found=3,
            missing_in_a=0,
            missing_in_b=1,
            content_differences=1,
            offset_differences=0,
            timestamp_differences=1,
            schema_id_differences=0,
            differences=self.differences
        )
    
    def test_get_severity(self):
        """Test determining severity of differences."""
        # Content differences should be ERROR
        self.assertEqual(self.reporter._get_severity(self.content_diff), DiffSeverity.ERROR)
        
        # Missing messages should be ERROR
        self.assertEqual(self.reporter._get_severity(self.missing_diff), DiffSeverity.ERROR)
        
        # Timestamp differences should be INFO
        self.assertEqual(self.reporter._get_severity(self.timestamp_diff), DiffSeverity.INFO)
        
        # Create a properly formatted schema ID difference
        message_a = create_test_message(b"test1", 0, 1000, schema_id=1)
        message_b = create_test_message(b"test1", 0, 1000, schema_id=2)
        
        schema_diff = MessageDifference(
            index=4,
            topic_a="topic-a",
            topic_b="topic-b",
            message_a=message_a,
            message_b=message_b,
            difference_type="schema_id",
            details="Schema ID mismatch: 1 != 2"
        )
        
        # Schema ID differences should be WARNING
        self.assertEqual(self.reporter._get_severity(schema_diff), DiffSeverity.WARNING)
    
    def test_filter_differences_by_severity(self):
        """Test filtering differences by severity."""
        # Set severity threshold to WARNING
        self.reporter.options.severity_threshold = DiffSeverity.WARNING
        
        # Filter differences
        filtered = self.reporter._filter_differences(self.differences)
        
        # Should include content and missing (both ERROR), but not timestamp (INFO)
        self.assertEqual(len(filtered), 2)
        self.assertIn(self.content_diff, filtered)
        self.assertIn(self.missing_diff, filtered)
        self.assertNotIn(self.timestamp_diff, filtered)
    
    def test_filter_differences_by_type(self):
        """Test filtering differences by type."""
        # Exclude content differences
        self.reporter.options.include_content_diffs = False
        
        # Filter differences
        filtered = self.reporter._filter_differences(self.differences)
        
        # Should include missing and timestamp, but not content
        self.assertEqual(len(filtered), 2)
        self.assertNotIn(self.content_diff, filtered)
        self.assertIn(self.missing_diff, filtered)
        self.assertIn(self.timestamp_diff, filtered)
        
        # Exclude missing messages too
        self.reporter.options.include_missing_messages = False
        
        # Filter differences
        filtered = self.reporter._filter_differences(self.differences)
        
        # Should include only timestamp
        self.assertEqual(len(filtered), 1)
        self.assertNotIn(self.content_diff, filtered)
        self.assertNotIn(self.missing_diff, filtered)
        self.assertIn(self.timestamp_diff, filtered)
    
    def test_filter_differences_max_details(self):
        """Test filtering differences with max_diff_details."""
        # Set max details to 2
        self.reporter.options.max_diff_details = 2
        
        # Filter differences
        filtered = self.reporter._filter_differences(self.differences)
        
        # Should include only the first 2 differences
        self.assertEqual(len(filtered), 2)
        self.assertIn(self.content_diff, filtered)
        self.assertIn(self.missing_diff, filtered)
        self.assertNotIn(self.timestamp_diff, filtered)
    
    def test_generate_text_report_summary(self):
        """Test generating text report with summary information."""
        # Generate report
        report = self.reporter.generate_report(self.result)
        
        # Check that the report contains expected information
        self.assertIn("KDIFF COMPARISON REPORT", report)
        self.assertIn("topic-a vs topic-b", report)
        self.assertIn("Messages compared: 100", report)
        self.assertIn("Differences found: 3", report)
        self.assertIn("Missing in topic-b: 1", report)
        self.assertIn("Content differences: 1", report)
        self.assertIn("Timestamp differences: 1", report)
        
        # Check the differences section
        self.assertIn("DIFFERENCES:", report)
        self.assertIn("Content difference at index 1", report)
        self.assertIn("Message at index 2 missing in topic-b", report)
        self.assertIn("Timestamp difference at index 3", report)
    
    def test_generate_text_report_verbosity(self):
        """Test generating text report with different verbosity levels."""
        # Set verbosity to minimal (0)
        self.reporter.options.verbosity = 0
        report_minimal = self.reporter.generate_report(self.result)
        
        # Set verbosity to normal (1)
        self.reporter.options.verbosity = 1
        report_normal = self.reporter.generate_report(self.result)
        
        # Set verbosity to detailed (2)
        self.reporter.options.verbosity = 2
        report_detailed = self.reporter.generate_report(self.result)
        
        # Set verbosity to debug (3)
        self.reporter.options.verbosity = 3
        report_debug = self.reporter.generate_report(self.result)
        
        # Check that reports have increasing detail
        self.assertLess(len(report_minimal), len(report_normal))
        self.assertLess(len(report_normal), len(report_detailed))
        
        # Check that detailed report includes comparison options
        self.assertIn("COMPARISON OPTIONS:", report_detailed)
        self.assertIn("Compare content: True", report_detailed)
        
        # Check that debug report includes content previews
        if b"test" in self.message_a1.content:  # Only if we have text content
            self.assertIn("Content preview:", report_debug)
    
    def test_generate_json_report(self):
        """Test generating JSON report."""
        # Set format to JSON
        self.reporter.options.format = ReportFormat.JSON
        
        # Generate report
        report = self.reporter.generate_report(self.result)
        
        # Parse report as JSON
        report_dict = json.loads(report)
        
        # Check structure
        self.assertEqual(report_dict["report_type"], "kdiff_comparison")
        self.assertIn("timestamp", report_dict)
        
        # Check comparison data
        comparison = report_dict["comparison"]
        self.assertEqual(comparison["topic_a"], "topic-a")
        self.assertEqual(comparison["topic_b"], "topic-b")
        self.assertEqual(comparison["messages_compared"], 100)
        self.assertEqual(comparison["differences_found"], 3)
        self.assertEqual(comparison["missing_in_b"], 1)
        self.assertEqual(comparison["content_differences"], 1)
        self.assertEqual(comparison["timestamp_differences"], 1)
        
        # Check options
        options = report_dict["options"]
        self.assertTrue(options["compare_content"])
        
        # Check differences
        differences = report_dict.get("differences", [])
        self.assertEqual(len(differences), 3)
        
        # Check severity counts
        severity_counts = report_dict.get("severity_counts", {})
        self.assertIn("ERROR", severity_counts)
        self.assertIn("INFO", severity_counts)
    
    def test_write_report_to_file(self):
        """Test writing report to file."""
        # Mock open
        mock_file = mock_open()
        
        # Use patch to mock open
        with patch('builtins.open', mock_file):
            # Write report to file
            self.reporter.write_report_to_file(self.result, "test_report.txt")
            
            # Check that file was opened for writing
            mock_file.assert_called_once_with("test_report.txt", "w")
            
            # Check that report was written to file
            handle = mock_file()
            handle.write.assert_called()


if __name__ == '__main__':
    unittest.main()
