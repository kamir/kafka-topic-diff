"""
Unit tests for the KDIFF topic reader.
"""

import unittest
from unittest.mock import MagicMock, patch, call, ANY
import time
from typing import Dict, List, Iterator, Any
from io import BytesIO

from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import TopicPartition

from kdiff.core.config import KDiffConfig
from kdiff.core.connector import KafkaConnector
from kdiff.core.reader import (
    TopicReader, KafkaMessage, MessageMeta, ReadProgress, TopicReaderError
)


def create_consumer_record(topic: str, partition: int, offset: int, value: bytes, 
                           timestamp: int = 1000, key: bytes = None, headers=None) -> ConsumerRecord:
    """Helper to create a ConsumerRecord for testing."""
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=len(key) if key else 0,
        serialized_value_size=len(value) if value else 0,
        headers=headers or [],
        leader_epoch=0,
        serialized_header_size=-1
    )


class MockTopicMetadata:
    """Mock for Kafka topic metadata."""
    
    def __init__(self, topic: str, partitions: List[int]):
        """Initialize with topic and partitions."""
        self.name = topic
        self.partitions = [MockPartitionMetadata(p) for p in partitions]


class MockPartitionMetadata:
    """Mock for Kafka partition metadata."""
    
    def __init__(self, partition_id: int):
        """Initialize with partition ID."""
        self.id = partition_id


class MockKafkaAdminClient:
    """Mock for KafkaAdminClient."""
    
    def __init__(self, topics_metadata: Dict[str, MockTopicMetadata] = None):
        """Initialize with optional topic metadata."""
        self.topics_metadata = topics_metadata or {}
    
    def describe_topics(self, topics: List[str]) -> Dict[str, Any]:
        """Mock for describe_topics."""
        return MockDescribeTopicsResponse(
            {t: self.topics_metadata.get(t) for t in topics if t in self.topics_metadata}
        )


class MockDescribeTopicsResponse:
    """Mock for Kafka describe_topics response."""
    
    def __init__(self, topics: Dict[str, MockTopicMetadata]):
        """Initialize with topics metadata."""
        self.topics = topics


class TestTopicReader(unittest.TestCase):
    """Tests for the TopicReader class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock the KDiffConfig
        self.config = MagicMock(spec=KDiffConfig)
        
        # Sample cluster configuration
        self.test_cluster_props = {
            "bootstrap.servers": "test-broker:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "test-user",
            "sasl.password": "test-password",
            "client.id": "test-client",
            "session.timeout.ms": "45000"
        }
        
        # Mock the KafkaConnector
        self.connector = MagicMock(spec=KafkaConnector)
        
        # Set up mock to return the test cluster properties
        self.config.get_cluster_config.return_value = self.test_cluster_props
        
        # Create reader with the mock config and connector
        self.reader = TopicReader(self.config, self.connector)
        
        # Test data
        self.test_topic = "test-topic"
        self.test_cluster = "test-cluster"
        self.test_partitions = [0, 1, 2]
        
        # Set up admin client mock
        self.admin_client = MockKafkaAdminClient({
            self.test_topic: MockTopicMetadata(self.test_topic, self.test_partitions)
        })
        self.connector.get_admin_client.return_value = self.admin_client
        
        # Set up consumer mock
        self.consumer = MagicMock()
        self.connector.get_consumer.return_value = self.consumer
        
        # Test messages
        self.messages = []
        for partition in self.test_partitions:
            for offset in range(10):
                # Create a message with a schema ID for testing
                value = bytearray([0, 0, 0, 0, 123])  # Magic byte + schema ID + content
                value[1:5] = (partition * 100 + offset).to_bytes(4, byteorder='big')
                
                self.messages.append(
                    create_consumer_record(
                        topic=self.test_topic,
                        partition=partition,
                        offset=offset,
                        value=bytes(value),
                        timestamp=1000 + offset * 1000,
                        key=f"key-{partition}-{offset}".encode() if offset % 2 == 0 else None,
                        headers=[("header1", b"value1"), ("header2", b"value2")] if offset % 3 == 0 else []
                    )
                )
    
    def test_extract_schema_id(self):
        """Test extracting schema ID from message."""
        # Create a message with a schema ID
        value = bytearray([0, 0, 0, 0, 123])  # Magic byte + schema ID + content
        value[1:5] = (42).to_bytes(4, byteorder='big')
        
        message = create_consumer_record(
            topic="test-topic",
            partition=0,
            offset=0,
            value=bytes(value)
        )
        
        schema_id = self.reader._extract_schema_id(message)
        self.assertEqual(schema_id, 42)
        
        # Test with invalid schema format
        message = create_consumer_record(
            topic="test-topic",
            partition=0,
            offset=0,
            value=b"not a schema"
        )
        
        schema_id = self.reader._extract_schema_id(message)
        self.assertIsNone(schema_id)
        
        # Test with empty message
        message = create_consumer_record(
            topic="test-topic",
            partition=0,
            offset=0,
            value=None
        )
        
        schema_id = self.reader._extract_schema_id(message)
        self.assertIsNone(schema_id)
    
    def test_extract_message_metadata(self):
        """Test extracting metadata from message."""
        # Create a message with headers and schema ID
        value = bytearray([0, 0, 0, 0, 123])  # Magic byte + schema ID + content
        value[1:5] = (42).to_bytes(4, byteorder='big')
        
        headers = [("header1", b"value1"), ("header2", b"value2")]
        
        message = create_consumer_record(
            topic="test-topic",
            partition=1,
            offset=100,
            timestamp=1234567890,
            key=b"test-key",
            value=bytes(value),
            headers=headers
        )
        
        meta = self.reader._extract_message_metadata(message)
        
        self.assertEqual(meta.offset, 100)
        self.assertEqual(meta.timestamp, 1234567890)
        self.assertEqual(meta.partition, 1)
        self.assertEqual(meta.key, b"test-key")
        self.assertEqual(meta.schema_id, 42)
        self.assertEqual(meta.headers, {"header1": b"value1", "header2": b"value2"})
    
    def test_convert_consumer_record(self):
        """Test converting a Kafka consumer record."""
        # Create a message with schema ID
        value = bytearray([0, 0, 0, 0, 123])  # Magic byte + schema ID + content
        value[1:5] = (42).to_bytes(4, byteorder='big')
        
        message = create_consumer_record(
            topic="test-topic",
            partition=0,
            offset=100,
            value=bytes(value)
        )
        
        kafka_message = self.reader._convert_consumer_record(message)
        
        self.assertIsInstance(kafka_message, KafkaMessage)
        self.assertEqual(kafka_message.content, message.value)
        self.assertEqual(kafka_message.meta.offset, 100)
        self.assertEqual(kafka_message.meta.partition, 0)
        self.assertEqual(kafka_message.meta.schema_id, 42)
    
    @patch('kdiff.core.reader.TopicReader._get_topic_partitions')
    @patch('kdiff.core.reader.TopicReader._read_partition')
    def test_read_topic_with_offsets(self, mock_read_partition, mock_get_partitions):
        """Test reading a topic with offset range."""
        # Set up mock for partition info
        mock_get_partitions.return_value = {
            0: {"earliest": 0, "latest": 100},
            1: {"earliest": 0, "latest": 200},
        }
        
        # Set up mock for reading a partition - return some test messages
        message1 = KafkaMessage(
            content=b"test1",
            meta=MessageMeta(offset=10, timestamp=1000, partition=0, key=None)
        )
        message2 = KafkaMessage(
            content=b"test2",
            meta=MessageMeta(offset=11, timestamp=2000, partition=0, key=None)
        )
        
        mock_read_partition.side_effect = [[message1, message2], []]
        
        # Read the topic with offset range
        messages = list(self.reader.read_topic(
            cluster_name=self.test_cluster,
            topic=self.test_topic,
            start_offset=10,
            end_offset=20
        ))
        
        # Check that the right methods were called
        mock_get_partitions.assert_called_once_with(self.test_cluster, self.test_topic)
        
        # Should have called _read_partition for each partition
        self.assertEqual(mock_read_partition.call_count, 2)
        
        # The first call should be for partition 0
        args0, kwargs0 = mock_read_partition.call_args_list[0]
        self.assertEqual(args0[0], self.test_cluster)
        self.assertEqual(args0[1], self.test_topic)
        self.assertEqual(args0[2], 0)  # partition
        self.assertEqual(args0[3], 10)  # start_offset
        self.assertEqual(args0[4], 20)  # end_offset
        
        # The second call should be for partition 1
        args1, kwargs1 = mock_read_partition.call_args_list[1]
        self.assertEqual(args1[0], self.test_cluster)
        self.assertEqual(args1[1], self.test_topic)
        self.assertEqual(args1[2], 1)  # partition
        self.assertEqual(args1[3], 10)  # start_offset
        self.assertEqual(args1[4], 20)  # end_offset
        
        # Check the messages
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], message1)
        self.assertEqual(messages[1], message2)
    
    @patch('kdiff.core.reader.TopicReader._get_topic_partitions')
    @patch('kdiff.core.reader.TopicReader._get_offsets_for_timestamps')
    @patch('kdiff.core.reader.TopicReader._read_partition')
    def test_read_topic_with_timestamps(self, mock_read_partition, mock_get_offsets, mock_get_partitions):
        """Test reading a topic with timestamp range."""
        # Set up mock for partition info
        mock_get_partitions.return_value = {
            0: {"earliest": 0, "latest": 100},
            1: {"earliest": 0, "latest": 200},
        }
        
        # Set up mock for getting offsets for timestamps
        mock_get_offsets.side_effect = [
            # Start timestamp offsets
            {0: 10, 1: 20},
            # End timestamp offsets
            {0: 50, 1: 60}
        ]
        
        # Set up mock for reading a partition - return some test messages
        message1 = KafkaMessage(
            content=b"test1",
            meta=MessageMeta(offset=10, timestamp=1000, partition=0, key=None)
        )
        message2 = KafkaMessage(
            content=b"test2",
            meta=MessageMeta(offset=20, timestamp=2000, partition=1, key=None)
        )
        
        mock_read_partition.side_effect = [[message1], [message2]]
        
        # Read the topic with timestamp range
        messages = list(self.reader.read_topic(
            cluster_name=self.test_cluster,
            topic=self.test_topic,
            start_timestamp=1000,
            end_timestamp=2000
        ))
        
        # Check that the right methods were called
        mock_get_partitions.assert_called_once_with(self.test_cluster, self.test_topic)
        
        # Should have called _get_offsets_for_timestamps twice (start and end)
        self.assertEqual(mock_get_offsets.call_count, 2)
        
        # Should have called _read_partition for each partition
        self.assertEqual(mock_read_partition.call_count, 2)
        
        # The first call should be for partition 0
        args0, kwargs0 = mock_read_partition.call_args_list[0]
        self.assertEqual(args0[0], self.test_cluster)
        self.assertEqual(args0[1], self.test_topic)
        self.assertEqual(args0[2], 0)  # partition
        self.assertEqual(args0[3], 10)  # start_offset from timestamp
        self.assertEqual(args0[4], 50)  # end_offset from timestamp
        
        # The second call should be for partition 1
        args1, kwargs1 = mock_read_partition.call_args_list[1]
        self.assertEqual(args1[0], self.test_cluster)
        self.assertEqual(args1[1], self.test_topic)
        self.assertEqual(args1[2], 1)  # partition
        self.assertEqual(args1[3], 20)  # start_offset from timestamp
        self.assertEqual(args1[4], 60)  # end_offset from timestamp
        
        # Check the messages
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], message1)
        self.assertEqual(messages[1], message2)
    
    @patch('kdiff.core.reader.TopicReader._get_topic_partitions')
    @patch('kdiff.core.reader.TopicReader._read_partition')
    def test_read_topic_with_max_messages(self, mock_read_partition, mock_get_partitions):
        """Test reading a topic with maximum message limit."""
        # Set up mock for partition info
        mock_get_partitions.return_value = {
            0: {"earliest": 0, "latest": 100},
            1: {"earliest": 0, "latest": 200},
        }
        
        # Set up mock for reading a partition - return some test messages
        messages = [
            KafkaMessage(
                content=f"test{i}".encode(),
                meta=MessageMeta(offset=i, timestamp=1000*i, partition=0, key=None)
            )
            for i in range(5)
        ]
        
        mock_read_partition.side_effect = [messages, []]
        
        # Read the topic with max messages limit
        result = list(self.reader.read_topic(
            cluster_name=self.test_cluster,
            topic=self.test_topic,
            max_messages=3
        ))
        
        # Should have called _read_partition only once (for partition 0)
        # since we got enough messages from the first partition
        self.assertEqual(mock_read_partition.call_count, 1)
        
        # Should have passed max_messages to _read_partition
        args0, kwargs0 = mock_read_partition.call_args
        self.assertEqual(args0[5], 3)  # max_messages
        
        # Check the messages - should only have 3
        self.assertEqual(len(result), 3)
        self.assertEqual(result, messages[:3])
    
    @patch('kdiff.core.reader.TopicReader._get_topic_partitions')
    @patch('kdiff.core.reader.TopicReader._read_partition')
    def test_read_topic_specific_partitions(self, mock_read_partition, mock_get_partitions):
        """Test reading from specific partitions."""
        # Set up mock for partition info
        mock_get_partitions.return_value = {
            0: {"earliest": 0, "latest": 100},
            1: {"earliest": 0, "latest": 200},
            2: {"earliest": 0, "latest": 300},
        }
        
        # Set up mock for reading a partition - return some test messages
        message1 = KafkaMessage(
            content=b"test1",
            meta=MessageMeta(offset=10, timestamp=1000, partition=0, key=None)
        )
        message2 = KafkaMessage(
            content=b"test2",
            meta=MessageMeta(offset=10, timestamp=2000, partition=2, key=None)
        )
        
        # We'll set the side effect to only return messages for partitions 0 and 2
        # which are the ones we'll be requesting in the test below
        mock_read_partition.side_effect = [[message1], [message2]]
        
        # Read the topic with specific partitions
        result = list(self.reader.read_topic(
            cluster_name=self.test_cluster,
            topic=self.test_topic,
            partitions=[0, 2]  # Skip partition 1
        ))
        
        # Should have called _read_partition twice (for partitions 0 and 2)
        self.assertEqual(mock_read_partition.call_count, 2)
        
        # Check the call args
        args0, kwargs0 = mock_read_partition.call_args_list[0]
        self.assertEqual(args0[2], 0)  # partition
        
        args1, kwargs1 = mock_read_partition.call_args_list[1]
        self.assertEqual(args1[2], 2)  # partition
        
        # Check the messages
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], message1)
        self.assertEqual(result[1], message2)
    
    def test_progress_reporting(self):
        """Test progress reporting during read operations."""
        # Create a mock progress callback
        progress_callback = MagicMock()
        self.reader.set_progress_callback(progress_callback)
        
        # Set up mock for get_topic_partitions
        self.reader._get_topic_partitions = MagicMock(return_value={
            0: {"earliest": 0, "latest": 100},
            1: {"earliest": 0, "latest": 200},
        })
        
        # Set up mock for read_partition
        messages = [
            KafkaMessage(
                content=f"test{i}".encode(),
                meta=MessageMeta(offset=i, timestamp=1000*i, partition=0, key=None)
            )
            for i in range(2000)  # Many messages to trigger progress updates
        ]
        
        self.reader._read_partition = MagicMock(side_effect=[messages, []])
        
        # Read the topic
        list(self.reader.read_topic(
            cluster_name=self.test_cluster,
            topic=self.test_topic
        ))
        
        # Should have called the progress callback multiple times
        # (initial, after each 1000 messages, after each partition, and final)
        self.assertGreater(progress_callback.call_count, 3)
        
        # Check the first call
        args, kwargs = progress_callback.call_args_list[0]
        progress = args[0]
        self.assertIsInstance(progress, ReadProgress)
        self.assertEqual(progress.topic, self.test_topic)
        self.assertEqual(progress.total_partitions, 2)
        # Don't assert completed_partitions as it depends on implementation details
        
        # Check the last call
        args, kwargs = progress_callback.call_args_list[-1]
        progress = args[0]
        self.assertEqual(progress.completed_partitions, 2)  # All partitions completed
        self.assertEqual(progress.messages_read, 2000)
    
    def test_read_partition(self):
        """Test reading messages from a partition."""
        # Create a consumer mock
        consumer = MagicMock()
        self.connector.get_consumer.return_value = consumer
        
        # Set up the consumer.poll behavior
        records = {}
        tp = TopicPartition(self.test_topic, 0)
        records[tp] = [
            create_consumer_record(
                topic=self.test_topic,
                partition=0,
                offset=i,
                value=f"test{i}".encode(),
                timestamp=1000 + i * 1000
            )
            for i in range(10, 20)
        ]
        
        # Consumer position for offset tracking
        position_values = list(range(10, 21))
        consumer.position.side_effect = position_values
        
        # Set up poll to return records first, then empty dict when called again
        consumer.poll.side_effect = [records, {}, {}]  # Add an extra empty dict to prevent StopIteration
        
        # Call _read_partition
        messages = list(self.reader._read_partition(
            cluster_name=self.test_cluster,
            topic=self.test_topic,
            partition=0,
            start_offset=10,
            end_offset=20
        ))
        
        # Check that the consumer was used correctly
        self.connector.get_consumer.assert_called_once_with(
            self.test_cluster,
            auto_offset_reset='earliest'
        )
        
        consumer.assign.assert_called_once()
        consumer.seek.assert_called_once()
        
        # Should have called poll three times (once for records, twice for empty results)
        self.assertEqual(consumer.poll.call_count, 3)
        
        # Check the messages
        self.assertEqual(len(messages), 10)
        for i, message in enumerate(messages):
            self.assertEqual(message.meta.offset, i + 10)
            self.assertEqual(message.content, f"test{i+10}".encode())
    
    def test_read_progress_properties(self):
        """Test ReadProgress properties."""
        progress = ReadProgress(
            topic="test-topic",
            total_partitions=4,
            completed_partitions=2,
            messages_read=1000,
            start_time=time.time() - 10,  # 10 seconds ago
            max_messages=2000
        )
        
        # Check percentage_complete
        self.assertEqual(progress.percentage_complete, 50.0)
        
        # Check elapsed_seconds
        self.assertAlmostEqual(progress.elapsed_seconds, 10.0, delta=0.1)
        
        # Check messages_per_second
        self.assertAlmostEqual(progress.messages_per_second, 100.0, delta=0.1)
        
        # Check string representation
        progress_str = str(progress)
        self.assertIn("50.0%", progress_str)
        self.assertIn("2/4 partitions", progress_str)
        self.assertIn("1000/2000 messages", progress_str)
    
    def test_close(self):
        """Test closing the reader."""
        self.reader.close()
        
        # Should have closed the executor and connector
        self.connector.close_all_connections.assert_called_once()


if __name__ == '__main__':
    unittest.main()
