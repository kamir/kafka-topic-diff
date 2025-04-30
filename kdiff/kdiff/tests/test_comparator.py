"""
Unit tests for the KDIFF comparator.
"""

import unittest
from unittest.mock import MagicMock, patch, call, ANY
import time
from typing import Dict, List, Iterator, Any
from io import BytesIO

from kdiff.core.config import KDiffConfig
from kdiff.core.reader import KafkaMessage, MessageMeta, TopicReader
from kdiff.core.comparator import (
    StreamComparator, TableComparator, ComparisonOptions, ComparisonResult, MessageDifference,
    ComparisonProgress, ComparisonError, ChecksumAlgorithm
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


class TestStreamComparator(unittest.TestCase):
    """Tests for the StreamComparator class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create default comparison options
        self.options = ComparisonOptions(
            compare_content=True,
            compare_offsets=False,
            compare_timestamps=False,
            compare_schema_ids=False
        )
        
        # Create comparator
        self.comparator = StreamComparator(options=self.options)
        
        # Create test messages
        self.message_a1 = create_test_message(b"test1", 0, 1000)
        self.message_a2 = create_test_message(b"test2", 1, 2000)
        self.message_a3 = create_test_message(b"test3", 2, 3000)
        
        self.message_b1 = create_test_message(b"test1", 0, 1000)
        self.message_b2 = create_test_message(b"different", 1, 2000)
        self.message_b3 = create_test_message(b"test3", 2, 3000)
    
    def test_compare_content_equal(self):
        """Test comparing equal content."""
        is_equal, details = self.comparator._compare_content(self.message_a1, self.message_b1)
        self.assertTrue(is_equal)
        self.assertEqual(details, "")
    
    def test_compare_content_different(self):
        """Test comparing different content."""
        is_equal, details = self.comparator._compare_content(self.message_a2, self.message_b2)
        self.assertFalse(is_equal)
        self.assertIn("Content mismatch", details)
    
    def test_compare_content_with_checksum(self):
        """Test comparing content using checksums."""
        # Set checksum algorithm
        self.comparator.options.checksum_algorithm = ChecksumAlgorithm.MD5
        
        # Equal content
        is_equal, details = self.comparator._compare_content(self.message_a1, self.message_b1)
        self.assertTrue(is_equal)
        self.assertEqual(details, "")
        
        # Different content
        is_equal, details = self.comparator._compare_content(self.message_a2, self.message_b2)
        self.assertFalse(is_equal)
        self.assertIn("Checksum mismatch", details)
    
    def test_compare_offsets_equal(self):
        """Test comparing equal offsets."""
        is_equal, details = self.comparator._compare_offsets(self.message_a1.meta, self.message_b1.meta)
        self.assertTrue(is_equal)
        self.assertEqual(details, "")
    
    def test_compare_offsets_different(self):
        """Test comparing different offsets."""
        # Create message with different offset
        message_b = create_test_message(b"test1", 10, 1000)
        
        is_equal, details = self.comparator._compare_offsets(self.message_a1.meta, message_b.meta)
        self.assertFalse(is_equal)
        self.assertIn("Offset mismatch", details)
    
    def test_compare_timestamps_equal(self):
        """Test comparing equal timestamps."""
        is_equal, details = self.comparator._compare_timestamps(self.message_a1.meta, self.message_b1.meta)
        self.assertTrue(is_equal)
        self.assertEqual(details, "")
    
    def test_compare_timestamps_different(self):
        """Test comparing different timestamps."""
        # Create message with different timestamp
        message_b = create_test_message(b"test1", 0, 1500)
        
        is_equal, details = self.comparator._compare_timestamps(self.message_a1.meta, message_b.meta)
        self.assertFalse(is_equal)
        self.assertIn("Timestamp mismatch", details)
    
    def test_compare_timestamps_with_tolerance(self):
        """Test comparing timestamps with tolerance."""
        # Set timestamp tolerance
        self.comparator.options.timestamp_tolerance_ms = 600
        
        # Create message with timestamp within tolerance
        message_b1 = create_test_message(b"test1", 0, 1500)
        
        # Create message with timestamp outside tolerance
        message_b2 = create_test_message(b"test1", 0, 1700)
        
        # Within tolerance should be equal
        is_equal, details = self.comparator._compare_timestamps(self.message_a1.meta, message_b1.meta)
        self.assertTrue(is_equal)
        
        # Outside tolerance should be different
        is_equal, details = self.comparator._compare_timestamps(self.message_a1.meta, message_b2.meta)
        self.assertFalse(is_equal)
        self.assertIn("Timestamp mismatch", details)
    
    def test_compare_schema_ids_equal(self):
        """Test comparing equal schema IDs."""
        # Create messages with schema IDs
        message_a = create_test_message(b"test1", 0, 1000, schema_id=42)
        message_b = create_test_message(b"test1", 0, 1000, schema_id=42)
        
        is_equal, details = self.comparator._compare_schema_ids(message_a.meta, message_b.meta)
        self.assertTrue(is_equal)
        self.assertEqual(details, "")
    
    def test_compare_schema_ids_different(self):
        """Test comparing different schema IDs."""
        # Create messages with different schema IDs
        message_a = create_test_message(b"test1", 0, 1000, schema_id=42)
        message_b = create_test_message(b"test1", 0, 1000, schema_id=43)
        
        is_equal, details = self.comparator._compare_schema_ids(message_a.meta, message_b.meta)
        self.assertFalse(is_equal)
        self.assertIn("Schema ID mismatch", details)
    
    def test_compare_messages_all_equal(self):
        """Test comparing messages with all fields equal."""
        # Enable all comparisons
        self.comparator.options.compare_offsets = True
        self.comparator.options.compare_timestamps = True
        self.comparator.options.compare_schema_ids = True
        
        # Create messages with all fields equal
        message_a = create_test_message(b"test1", 0, 1000, schema_id=42)
        message_b = create_test_message(b"test1", 0, 1000, schema_id=42)
        
        differences = self.comparator._compare_messages(0, message_a, message_b, "topic-a", "topic-b")
        self.assertEqual(len(differences), 0)
    
    def test_compare_messages_with_differences(self):
        """Test comparing messages with differences."""
        # Enable all comparisons
        self.comparator.options.compare_offsets = True
        self.comparator.options.compare_timestamps = True
        self.comparator.options.compare_schema_ids = True
        
        # Create messages with differences in all fields
        message_a = create_test_message(b"test1", 0, 1000, schema_id=42)
        message_b = create_test_message(b"test2", 1, 2000, schema_id=43)
        
        differences = self.comparator._compare_messages(0, message_a, message_b, "topic-a", "topic-b")
        self.assertEqual(len(differences), 4)  # Content, offset, timestamp, schema_id
        
        # Check difference types
        difference_types = [diff.difference_type for diff in differences]
        self.assertIn("content", difference_types)
        self.assertIn("offset", difference_types)
        self.assertIn("timestamp", difference_types)
        self.assertIn("schema_id", difference_types)
    
    def test_compare_streams_identical(self):
        """Test comparing identical streams."""
        # Create identical streams
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_a2, self.message_a3])  # Using message_a2 and message_a3 for both
        
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b")
        
        self.assertEqual(result.messages_compared, 3)
        self.assertEqual(result.differences_found, 0)
        self.assertEqual(result.missing_in_a, 0)
        self.assertEqual(result.missing_in_b, 0)
        self.assertEqual(result.content_differences, 0)
    
    def test_compare_streams_with_differences(self):
        """Test comparing streams with differences."""
        # Create streams with differences
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_b2, self.message_b3])
        
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b")
        
        self.assertEqual(result.messages_compared, 3)
        self.assertEqual(result.differences_found, 1)
        self.assertEqual(result.missing_in_a, 0)
        self.assertEqual(result.missing_in_b, 0)
        self.assertEqual(result.content_differences, 1)
    
    def test_compare_streams_different_lengths(self):
        """Test comparing streams with different lengths."""
        # Create streams with different lengths
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_b2])
        
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b")
        
        self.assertEqual(result.messages_compared, 3)
        self.assertEqual(result.differences_found, 2)  # 1 content difference + 1 missing
        self.assertEqual(result.missing_in_a, 0)
        self.assertEqual(result.missing_in_b, 1)
        self.assertEqual(result.content_differences, 1)
    
    def test_compare_streams_with_max_messages(self):
        """Test comparing streams with maximum message limit."""
        # Create streams
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_b2, self.message_b3])
        
        # Compare with max_messages=2
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b", max_messages=2)
        
        self.assertEqual(result.messages_compared, 2)
        self.assertEqual(result.differences_found, 1)
        self.assertEqual(result.content_differences, 1)
    
    def test_compare_streams_with_max_differences(self):
        """Test comparing streams with maximum differences limit."""
        # Set max_differences
        self.comparator.options.max_differences = 1
        
        # Create streams with multiple differences
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_b2, create_test_message(b"different3", 2, 3000)])
        
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b")
        
        # Should stop after finding the first difference
        self.assertEqual(result.differences_found, 1)
    
    def test_compare_streams_with_progress_callback(self):
        """Test comparing streams with progress callback."""
        # Create mock progress callback
        progress_callback = MagicMock()
        self.comparator.set_progress_callback(progress_callback)
        
        # Create streams
        stream_a = iter([self.message_a1, self.message_a2, self.message_a3])
        stream_b = iter([self.message_b1, self.message_b2, self.message_b3])
        
        # Compare streams
        result = self.comparator.compare_streams(stream_a, stream_b, "topic-a", "topic-b")
        
        # Should have called the progress callback at least once
        self.assertGreater(progress_callback.call_count, 0)
        
        # Check the progress object
        args, kwargs = progress_callback.call_args
        progress = args[0]
        self.assertIsInstance(progress, ComparisonProgress)
        self.assertEqual(progress.topic_a, "topic-a")
        self.assertEqual(progress.topic_b, "topic-b")
        self.assertEqual(progress.messages_compared, 3)
    
    def test_compare_topics(self):
        """Test comparing topics using the topic reader."""
        # Create mock reader
        reader = MagicMock(spec=TopicReader)
        
        # Set up mock for read_topic
        reader.read_topic.side_effect = [
            iter([self.message_a1, self.message_a2, self.message_a3]),  # First call for topic A
            iter([self.message_b1, self.message_b2, self.message_b3])   # Second call for topic B
        ]
        
        # Compare topics
        result = self.comparator.compare_topics(
            reader=reader,
            cluster_a="cluster-a",
            topic_a="topic-a",
            cluster_b="cluster-b",
            topic_b="topic-b",
            start_offset_a=0,
            end_offset_a=100,
            start_offset_b=0,
            end_offset_b=100,
            max_messages=10
        )
        
        # Check that read_topic was called correctly
        self.assertEqual(reader.read_topic.call_count, 2)
        
        # Check the result
        self.assertEqual(result.messages_compared, 3)
        self.assertEqual(result.differences_found, 1)
        self.assertEqual(result.content_differences, 1)
    
    def test_comparison_result_string(self):
        """Test string representation of comparison result."""
        # Create a result
        result = ComparisonResult(
            topic_a="topic-a",
            topic_b="topic-b",
            messages_compared=100,
            differences_found=5,
            missing_in_a=1,
            missing_in_b=2,
            content_differences=2,
            offset_differences=0,
            timestamp_differences=0,
            schema_id_differences=0,
            differences=[],
            start_time=time.time() - 10,
            end_time=time.time(),
            options=self.options
        )
        
        # Convert to string
        result_str = str(result)
        
        # Check that the string contains expected information
        self.assertIn("topic-a vs topic-b", result_str)
        self.assertIn("Messages compared: 100", result_str)
        self.assertIn("Differences found: 5", result_str)
        self.assertIn("Missing in topic-a: 1", result_str)
        self.assertIn("Missing in topic-b: 2", result_str)
        self.assertIn("Content differences: 2", result_str)


class TestTableComparator(unittest.TestCase):
    """Tests for the TableComparator class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create default comparison options
        self.options = ComparisonOptions(
            compare_content=True,
            compare_offsets=False,
            compare_timestamps=False,
            compare_schema_ids=False
        )
        
        # Create comparator
        self.comparator = TableComparator(options=self.options)
        
        # Create test messages with different content
        self.message_a1 = create_test_message(b"test1", 0, 1000)
        self.message_a2 = create_test_message(b"test2", 1, 2000)
        self.message_a3 = create_test_message(b"test3", 2, 3000)
        self.message_a4 = create_test_message(b"test4", 3, 4000)
        
        # Create equivalent messages for topic B (but with different offsets/timestamps)
        self.message_b1 = create_test_message(b"test1", 10, 1500)  # Same content, different offset/timestamp
        self.message_b3 = create_test_message(b"test3", 12, 3500)  # Same content, different offset/timestamp
        self.message_b4 = create_test_message(b"test4", 13, 4500)  # Same content, different offset/timestamp
        self.message_b5 = create_test_message(b"test5", 14, 5000)  # Unique to topic B
        
        # Create a duplicate message for topic B
        self.message_b3_duplicate = create_test_message(b"test3", 15, 3600)  # Duplicate of message_b3
    
    def test_get_message_key(self):
        """Test getting message keys for comparison."""
        # Test normal key generation (content-based)
        key1 = self.comparator._get_message_key(self.message_a1)
        key2 = self.comparator._get_message_key(self.message_b1)
        
        # Same content should produce same key
        self.assertEqual(key1, key2)
        
        # Different content should produce different keys
        key3 = self.comparator._get_message_key(self.message_a2)
        self.assertNotEqual(key1, key3)
        
        # Test with checksum algorithm
        self.comparator.options.checksum_algorithm = ChecksumAlgorithm.MD5
        checksum_key1 = self.comparator._get_message_key(self.message_a1)
        checksum_key2 = self.comparator._get_message_key(self.message_b1)
        
        # Same content should produce same checksum
        self.assertEqual(checksum_key1, checksum_key2)
        
        # Different content should produce different checksums
        checksum_key3 = self.comparator._get_message_key(self.message_a2)
        self.assertNotEqual(checksum_key1, checksum_key3)
    
    def test_compare_identical_collections(self):
        """Test comparing collections with identical content."""
        # Create collections with identical content
        messages_a = iter([self.message_a1, self.message_a3, self.message_a4])
        messages_b = iter([self.message_b1, self.message_b3, self.message_b4])  # Same content, different offsets
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should find no content differences
        self.assertEqual(result.differences_found, 0)
        self.assertEqual(result.missing_in_a, 0)
        self.assertEqual(result.missing_in_b, 0)
        self.assertEqual(result.content_differences, 0)
    
    def test_compare_with_metadata_differences(self):
        """Test comparing collections with metadata differences."""
        # Enable all metadata comparisons
        self.comparator.options.compare_offsets = True
        self.comparator.options.compare_timestamps = True
        
        # Create collections with identical content but different metadata
        messages_a = iter([self.message_a1, self.message_a3])
        messages_b = iter([self.message_b1, self.message_b3])  # Same content, different offsets/timestamps
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should find metadata differences but no content differences
        self.assertEqual(result.content_differences, 0)
        self.assertEqual(result.offset_differences, 2)  # Both messages have different offsets
        self.assertEqual(result.timestamp_differences, 2)  # Both messages have different timestamps
    
    def test_compare_with_missing_messages(self):
        """Test comparing collections with missing messages."""
        # Create collections with missing messages
        messages_a = iter([self.message_a1, self.message_a2, self.message_a3])
        messages_b = iter([self.message_b1, self.message_b3, self.message_b5])  # Missing a2, has b5 instead
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should find one message missing in each topic
        self.assertEqual(result.missing_in_a, 1)  # message_b5 content missing in topic A
        self.assertEqual(result.missing_in_b, 1)  # message_a2 content missing in topic B
        self.assertEqual(result.differences_found, 2)  # Two differences (one in each direction)
    
    def test_compare_with_duplicates(self):
        """Test comparing collections with duplicate messages."""
        # Create collections with duplicates
        messages_a = iter([self.message_a1, self.message_a3])
        messages_b = iter([self.message_b1, self.message_b3, self.message_b3_duplicate])  # Contains duplicate
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should find a duplicate in topic B and report it
        has_duplicate_note = False
        for diff in result.differences:
            if diff.difference_type == "duplicate" and "duplicate messages in topic-b" in diff.details:
                has_duplicate_note = True
                break
                
        self.assertTrue(has_duplicate_note, "Should include a note about duplicates in differences")
    
    def test_compare_with_progress_callback(self):
        """Test table comparison with progress callback."""
        # Create mock progress callback
        progress_callback = MagicMock()
        self.comparator.set_progress_callback(progress_callback)
        
        # Create collections
        messages_a = iter([self.message_a1, self.message_a2, self.message_a3])
        messages_b = iter([self.message_b1, self.message_b5])
        
        # Compare collections
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should have called the progress callback at least once
        self.assertGreater(progress_callback.call_count, 0)
        
        # Check progress object
        args, kwargs = progress_callback.call_args
        progress = args[0]
        self.assertIsInstance(progress, ComparisonProgress)
        self.assertEqual(progress.topic_a, "topic-a")
        self.assertEqual(progress.topic_b, "topic-b")
    
    def test_compare_with_checksum(self):
        """Test comparing collections using checksums."""
        # Set checksum algorithm
        self.comparator.options.checksum_algorithm = ChecksumAlgorithm.SHA256
        
        # Create collections
        messages_a = iter([self.message_a1, self.message_a2])
        messages_b = iter([self.message_b1, create_test_message(b"test2", 11, 2500)])  # Same content
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should find no differences when using checksums
        self.assertEqual(result.differences_found, 0)
        self.assertEqual(result.content_differences, 0)
    
    def test_large_message_sets(self):
        """Test comparing larger sets of messages for performance."""
        # Create larger sets of messages
        large_set_a = []
        large_set_b = []
        
        # Create 100 messages for each set with 90% overlap
        for i in range(100):
            msg_a = create_test_message(f"message{i}".encode(), i, i * 1000)
            large_set_a.append(msg_a)
            
            if i < 90:  # 90% overlap
                # Same content, different metadata
                msg_b = create_test_message(f"message{i}".encode(), i + 1000, i * 1100)
                large_set_b.append(msg_b)
            
        # Add 10 unique messages to set B
        for i in range(100, 110):
            msg_b = create_test_message(f"unique{i}".encode(), i + 1000, i * 1000)
            large_set_b.append(msg_b)
        
        # Compare large sets
        start_time = time.time()
        result = self.comparator.compare(
            "topic-a", iter(large_set_a),
            "topic-b", iter(large_set_b)
        )
        elapsed = time.time() - start_time
        
        # Verify results
        self.assertEqual(result.missing_in_a, 10)  # 10 messages unique to B
        self.assertEqual(result.missing_in_b, 10)  # 10 messages unique to A (90-99)
        self.assertEqual(result.messages_compared, 200)  # Total messages processed (100 from A + 100 from B)
        
        # Performance should be reasonable
        self.assertLess(elapsed, 2.0, "Large set comparison should complete in reasonable time")
    
    def test_compare_with_max_differences(self):
        """Test comparing with maximum differences limit."""
        # Set max differences
        self.comparator.options.max_differences = 2
        
        # Create collections with multiple differences
        messages_a = iter([self.message_a1, self.message_a2, self.message_a3, self.message_a4])
        messages_b = iter([self.message_b1, self.message_b5])  # Missing a2, a3, a4, but has b5
        
        result = self.comparator.compare(
            "topic-a", messages_a,
            "topic-b", messages_b
        )
        
        # Should stop after finding max_differences
        self.assertEqual(result.differences_found, 2)  # Capped at 2
        # Note: actual logical differences would be 4 (3 missing in B, 1 missing in A)


if __name__ == '__main__':
    unittest.main()
