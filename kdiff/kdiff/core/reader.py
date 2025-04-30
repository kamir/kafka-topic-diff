"""
Topic reader module for KDIFF.

This module provides functionality to read messages from Kafka topics
based on specified boundaries (offset ranges, timestamp ranges).
"""

import logging
import time
from typing import Dict, Any, Optional, List, Iterator, Tuple, Set, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError, IllegalStateError

from kdiff.core.config import KDiffConfig
from kdiff.core.connector import KafkaConnector, KafkaConnectionError

logger = logging.getLogger(__name__)


class TopicReaderError(Exception):
    """Exception raised for topic reader errors."""
    pass


@dataclass
class MessageMeta:
    """Metadata about a Kafka message."""
    
    offset: int
    timestamp: int
    partition: int
    key: Optional[bytes]
    schema_id: Optional[int] = None
    headers: Optional[Dict[str, bytes]] = None


@dataclass
class KafkaMessage:
    """Representation of a message from a Kafka topic."""
    
    content: bytes
    meta: MessageMeta


@dataclass
class ReadProgress:
    """Progress information for a topic read operation."""
    
    topic: str
    total_partitions: int
    completed_partitions: int
    messages_read: int
    start_time: float
    max_messages: Optional[int] = None
    
    @property
    def percentage_complete(self) -> float:
        """
        Get the percentage of completion.
        
        Returns:
            Percentage complete (0-100)
        """
        if self.total_partitions == 0:
            return 0.0
        
        if self.max_messages and self.messages_read >= self.max_messages:
            return 100.0
            
        return (self.completed_partitions / self.total_partitions) * 100.0
    
    @property
    def elapsed_seconds(self) -> float:
        """
        Get the elapsed time in seconds.
        
        Returns:
            Elapsed time in seconds
        """
        return time.time() - self.start_time
    
    @property
    def messages_per_second(self) -> float:
        """
        Get the messages read per second.
        
        Returns:
            Messages per second
        """
        if self.elapsed_seconds == 0:
            return 0.0
        
        return self.messages_read / self.elapsed_seconds
    
    def __str__(self) -> str:
        """
        Get a string representation of the progress.
        
        Returns:
            String representation
        """
        percentage = f"{self.percentage_complete:.1f}%"
        partitions = f"{self.completed_partitions}/{self.total_partitions} partitions"
        messages = f"{self.messages_read} messages"
        if self.max_messages:
            messages = f"{self.messages_read}/{self.max_messages} messages"
        
        rate = f"{self.messages_per_second:.1f} msg/s"
        elapsed = f"{self.elapsed_seconds:.1f}s elapsed"
        
        return f"{percentage} ({partitions}) - {messages} - {rate} - {elapsed}"


class TopicReader:
    """
    Reader for Kafka topics.
    
    This class handles reading messages from Kafka topics based on specified
    boundaries (offset ranges, timestamp ranges) and extracting metadata.
    """
    
    def __init__(self, config: KDiffConfig, connector: Optional[KafkaConnector] = None):
        """
        Initialize the topic reader.
        
        Args:
            config: The KDiff configuration
            connector: Optional Kafka connector (created if not provided)
        """
        self.config = config
        self.connector = connector or KafkaConnector(config)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.message_count_lock = Lock()
        self.progress_callback: Optional[Callable[[ReadProgress], None]] = None
    
    def set_progress_callback(self, callback: Callable[[ReadProgress], None]):
        """
        Set a callback function to report progress.
        
        Args:
            callback: Function to call with progress updates
        """
        self.progress_callback = callback
    
    def _extract_schema_id(self, message: ConsumerRecord) -> Optional[int]:
        """
        Extract the schema ID from a message.
        
        Args:
            message: Kafka consumer record
            
        Returns:
            Schema ID or None if not found
        """
        # Schema registry typically adds schema ID to the first few bytes of the value
        # The format is usually: Magic byte (1 byte) + Schema ID (4 bytes) + Avro data
        try:
            if not message.value or len(message.value) < 5:
                return None
                
            # Check for magic byte (0)
            if message.value[0] != 0:
                return None
                
            # Extract schema ID (4 bytes, big endian)
            schema_id = int.from_bytes(message.value[1:5], byteorder='big')
            return schema_id
        except Exception:
            return None
    
    def _extract_message_metadata(self, message: ConsumerRecord) -> MessageMeta:
        """
        Extract metadata from a Kafka message.
        
        Args:
            message: Kafka consumer record
            
        Returns:
            Message metadata
        """
        # Convert headers from list of tuples to dict if present
        headers = None
        if message.headers:
            headers = {key: value for key, value in message.headers}
        
        return MessageMeta(
            offset=message.offset,
            timestamp=message.timestamp,
            partition=message.partition,
            key=message.key,
            schema_id=self._extract_schema_id(message),
            headers=headers
        )
    
    def _convert_consumer_record(self, record: ConsumerRecord) -> KafkaMessage:
        """
        Convert a Kafka consumer record to a KafkaMessage.
        
        Args:
            record: Kafka consumer record
            
        Returns:
            KafkaMessage
        """
        meta = self._extract_message_metadata(record)
        
        return KafkaMessage(
            content=record.value,
            meta=meta
        )
    
    def _get_topic_partitions(self, cluster_name: str, topic: str) -> Dict[int, Dict[str, int]]:
        """
        Get information about topic partitions.
        
        Args:
            cluster_name: Name of the cluster
            topic: Topic name
            
        Returns:
            Dictionary of partition information
            
        Raises:
            TopicReaderError: If the topic information cannot be retrieved
        """
        try:
            # Get admin client
            admin = self.connector.get_admin_client(cluster_name)
            
            # Get topic partitions
            topic_partitions = {}
            
            # Describe topics (get metadata)
            topic_metadata = admin.describe_topics([topic])
            
            # In the kafka-python API, describe_topics returns a list of topic metadata
            # not an object with a 'topics' attribute
            found_topic = None
            for metadata in topic_metadata:
                if metadata.get('topic') == topic:
                    found_topic = metadata
                    break
                    
            if not found_topic:
                raise TopicReaderError(f"Topic '{topic}' not found in cluster '{cluster_name}'")
                
            partitions = found_topic.get('partitions', [])
            
            for partition in partitions:
                partition_id = partition.get('partition')
                topic_partitions[partition_id] = {
                    "earliest": 0,
                    "latest": 0
                }
            
            # Get beginning and end offsets for each partition
            consumer = self.connector.get_consumer(cluster_name)
            
            # Create TopicPartition objects
            from kafka.structs import TopicPartition
            tp_list = [TopicPartition(topic, p) for p in topic_partitions.keys()]
            
            # Get beginning offsets
            beginning_offsets = consumer.beginning_offsets(tp_list)
            # Get end offsets
            end_offsets = consumer.end_offsets(tp_list)
            
            # Update topic_partitions
            for tp, offset in beginning_offsets.items():
                topic_partitions[tp.partition]["earliest"] = offset
                
            for tp, offset in end_offsets.items():
                topic_partitions[tp.partition]["latest"] = offset
            
            return topic_partitions
        except KafkaError as e:
            logger.error(f"Kafka error getting topic partitions: {e}")
            raise TopicReaderError(f"Failed to get partitions for topic '{topic}': {e}")
        except Exception as e:
            logger.exception(f"Error getting topic partitions: {e}")
            raise TopicReaderError(f"Failed to get partitions for topic '{topic}': {e}")
    
    def _get_offsets_for_timestamps(self, 
                                   cluster_name: str, 
                                   topic: str, 
                                   partitions: List[int], 
                                   timestamp: int) -> Dict[int, int]:
        """
        Get offsets for a specific timestamp.
        
        Args:
            cluster_name: Name of the cluster
            topic: Topic name
            partitions: List of partition IDs
            timestamp: Timestamp to find offsets for (epoch ms)
            
        Returns:
            Dictionary mapping partition IDs to offsets
            
        Raises:
            TopicReaderError: If the offsets cannot be retrieved
        """
        try:
            consumer = self.connector.get_consumer(cluster_name)
            
            # Create timestamp map for each partition
            from kafka.structs import TopicPartition
            timestamps = {TopicPartition(topic, p): timestamp for p in partitions}
            
            # Get offsets for timestamps
            offsets_for_times = consumer.offsets_for_times(timestamps)
            
            # Convert to simpler structure
            result = {}
            for tp, offset_and_timestamp in offsets_for_times.items():
                if offset_and_timestamp is not None:
                    result[tp.partition] = offset_and_timestamp.offset
                else:
                    # No messages with timestamp >= requested timestamp
                    result[tp.partition] = -1
            
            return result
        except KafkaError as e:
            logger.error(f"Kafka error getting offsets for timestamps: {e}")
            raise TopicReaderError(f"Failed to get offsets for timestamp {timestamp}: {e}")
        except Exception as e:
            logger.exception(f"Error getting offsets for timestamps: {e}")
            raise TopicReaderError(f"Failed to get offsets for timestamp {timestamp}: {e}")
    
    def _read_partition(self, 
                       cluster_name: str, 
                       topic: str, 
                       partition: int, 
                       start_offset: int, 
                       end_offset: int, 
                       max_messages: Optional[int] = None,
                       message_count: List[int] = None) -> Iterator[KafkaMessage]:
        """
        Read messages from a specific partition.
        
        Args:
            cluster_name: Name of the cluster
            topic: Topic name
            partition: Partition ID
            start_offset: Start offset (inclusive)
            end_offset: End offset (exclusive)
            max_messages: Maximum number of messages to read
            message_count: Shared list to track message count across partitions
            
        Returns:
            Iterator of KafkaMessages
            
        Raises:
            TopicReaderError: If the messages cannot be read
        """
        try:
            # Adjust end_offset if it's -1 (indicating no messages)
            if end_offset == -1:
                logger.debug(f"No messages to read in partition {partition} (end offset is -1)")
                return
                
            # Skip if start_offset >= end_offset
            if start_offset >= end_offset:
                logger.debug(f"No messages to read in partition {partition} (start offset >= end offset)")
                return
                
            # Get a consumer
            from kafka.structs import TopicPartition
            tp = TopicPartition(topic, partition)
            
            # Create consumer with auto_offset_reset='earliest' to avoid errors when seeking
            consumer = self.connector.get_consumer(cluster_name, auto_offset_reset='earliest')
            
            # Assign to partition
            consumer.assign([tp])
            
            # Seek to start offset
            consumer.seek(tp, start_offset)
            
            # Read messages until the end offset or max messages
            message_count_for_partition = 0
            messages_yielded = 0
            poll_timeout_ms = 1000  # 1 second
            
            while True:
                # Check if we've reached the max messages
                if message_count is not None and sum(message_count) >= max_messages:
                    logger.debug(f"Reached max messages {max_messages} across all partitions")
                    break
                    
                if max_messages is not None and messages_yielded >= max_messages:
                    logger.debug(f"Reached max messages {max_messages} for partition {partition}")
                    break
                
                # Get the current position
                current_offset = consumer.position(tp)
                
                # Check if we've reached the end offset
                if current_offset >= end_offset:
                    logger.debug(f"Reached end offset {end_offset} for partition {partition}")
                    break
                
                # Poll for messages
                poll_records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=min(500, end_offset - current_offset))
                
                # No more messages available
                if not poll_records:
                    # Try once more with a longer timeout
                    poll_records = consumer.poll(timeout_ms=5000, max_records=min(500, end_offset - current_offset))
                    if not poll_records:
                        logger.debug(f"No more messages available in partition {partition}")
                        break
                
                # Process records for our partition
                if tp in poll_records:
                    records = poll_records[tp]
                    
                    for record in records:
                        # Skip records outside our range
                        if record.offset < start_offset:
                            continue
                            
                        if record.offset >= end_offset:
                            break
                            
                        # Check if we've reached max messages
                        if max_messages is not None:
                            if message_count is not None:
                                with self.message_count_lock:
                                    if sum(message_count) >= max_messages:
                                        break
                                    message_count[0] += 1
                                    message_count_for_partition += 1
                            elif messages_yielded >= max_messages:
                                break
                        
                        # Convert and yield the message
                        kafka_message = self._convert_consumer_record(record)
                        yield kafka_message
                        
                        messages_yielded += 1
                        
                        # Check if we've reached the end offset
                        if record.offset + 1 >= end_offset:
                            break
        except KafkaError as e:
            logger.error(f"Kafka error reading partition {partition}: {e}")
            raise TopicReaderError(f"Failed to read from partition {partition}: {e}")
        except Exception as e:
            logger.exception(f"Error reading partition {partition}: {e}")
            raise TopicReaderError(f"Failed to read from partition {partition}: {e}")
    
    def read_topic(self, 
                  cluster_name: str, 
                  topic: str, 
                  start_offset: Optional[int] = None, 
                  end_offset: Optional[int] = None,
                  start_timestamp: Optional[int] = None, 
                  end_timestamp: Optional[int] = None,
                  max_messages: Optional[int] = None,
                  partitions: Optional[List[int]] = None) -> Iterator[KafkaMessage]:
        """
        Read messages from a topic based on specified criteria.
        
        Args:
            cluster_name: Name of the cluster
            topic: Topic name
            start_offset: Start offset (inclusive), applied to each partition
            end_offset: End offset (exclusive), applied to each partition
            start_timestamp: Start timestamp (inclusive), used to find start offsets
            end_timestamp: End timestamp (exclusive), used to find end offsets
            max_messages: Maximum number of messages to read
            partitions: Specific partitions to read from (or all if None)
            
        Returns:
            Iterator of KafkaMessages
            
        Raises:
            TopicReaderError: If the messages cannot be read
        """
        try:
            # Get topic partition information
            topic_partitions = self._get_topic_partitions(cluster_name, topic)
            
            if not topic_partitions:
                logger.warning(f"No partitions found for topic '{topic}'")
                return
                
            # Filter to specific partitions if requested
            if partitions:
                topic_partitions = {p: info for p, info in topic_partitions.items() if p in partitions}
                
            if not topic_partitions:
                logger.warning(f"No matching partitions found for topic '{topic}'")
                return
                
            # Determine start offsets based on timestamp if provided
            partition_start_offsets = {}
            if start_timestamp is not None:
                timestamp_offsets = self._get_offsets_for_timestamps(
                    cluster_name,
                    topic,
                    list(topic_partitions.keys()),
                    start_timestamp
                )
                
                for partition, offset in timestamp_offsets.items():
                    if offset != -1:  # -1 indicates no messages with timestamp >= requested timestamp
                        partition_start_offsets[partition] = offset
                    else:
                        # Use the latest offset (effectively skipping this partition)
                        partition_start_offsets[partition] = topic_partitions[partition]["latest"]
            else:
                # Use start_offset if provided, otherwise use earliest
                for partition, info in topic_partitions.items():
                    if start_offset is not None:
                        # Clamp to valid range
                        partition_start_offsets[partition] = max(
                            start_offset,
                            info["earliest"]
                        )
                    else:
                        partition_start_offsets[partition] = info["earliest"]
            
            # Determine end offsets based on timestamp if provided
            partition_end_offsets = {}
            if end_timestamp is not None:
                timestamp_offsets = self._get_offsets_for_timestamps(
                    cluster_name,
                    topic,
                    list(topic_partitions.keys()),
                    end_timestamp
                )
                
                for partition, offset in timestamp_offsets.items():
                    if offset != -1:  # -1 indicates no messages with timestamp >= requested timestamp
                        partition_end_offsets[partition] = offset
                    else:
                        # Use the latest offset
                        partition_end_offsets[partition] = topic_partitions[partition]["latest"]
            else:
                # Use end_offset if provided, otherwise use latest
                for partition, info in topic_partitions.items():
                    if end_offset is not None:
                        # Clamp to valid range
                        partition_end_offsets[partition] = min(
                            end_offset,
                            info["latest"]
                        )
                    else:
                        partition_end_offsets[partition] = info["latest"]
            
            # Set up progress tracking
            progress = ReadProgress(
                topic=topic,
                total_partitions=len(topic_partitions),
                completed_partitions=0,
                messages_read=0,
                start_time=time.time(),
                max_messages=max_messages
            )
            
            # Shared counter for tracking the total message count across partitions
            message_count = [0]
            
            # Start reading from partitions
            for partition in sorted(topic_partitions.keys()):
                # Skip partitions with no messages to read
                start_offset = partition_start_offsets.get(partition, 0)
                end_offset = partition_end_offsets.get(partition, 0)
                
                if start_offset >= end_offset:
                    progress.completed_partitions += 1
                    if self.progress_callback:
                        self.progress_callback(progress)
                    continue
                
                logger.debug(f"Reading partition {partition} from offset {start_offset} to {end_offset}")
                
                # Read messages from this partition
                try:
                    for message in self._read_partition(
                        cluster_name,
                        topic,
                        partition,
                        start_offset,
                        end_offset,
                        max_messages,
                        message_count
                    ):
                        # Update progress
                        progress.messages_read += 1
                        
                        # Report progress periodically
                        if self.progress_callback and progress.messages_read % 1000 == 0:
                            self.progress_callback(progress)
                            
                        # Yield the message
                        yield message
                        
                        # Check if we've reached max messages
                        if max_messages is not None and progress.messages_read >= max_messages:
                            logger.debug(f"Reached max messages {max_messages}")
                            break
                except Exception as e:
                    logger.error(f"Error reading partition {partition}: {e}")
                    # Continue with next partition
                
                # Update progress
                progress.completed_partitions += 1
                if self.progress_callback:
                    self.progress_callback(progress)
                    
                # Check if we've reached max messages
                if max_messages is not None and progress.messages_read >= max_messages:
                    logger.debug(f"Reached max messages {max_messages}")
                    break
            
            # Final progress update
            if self.progress_callback:
                self.progress_callback(progress)
                
        except Exception as e:
            logger.exception(f"Error reading topic '{topic}': {e}")
            raise TopicReaderError(f"Failed to read from topic '{topic}': {e}")
    
    def close(self):
        """Close the topic reader and release resources."""
        if self.executor:
            self.executor.shutdown(wait=False)
            
        if self.connector:
            self.connector.close_all_connections()
    
    def __del__(self):
        """Cleanup when the reader is garbage collected."""
        try:
            self.close()
        except Exception:
            # Ignore errors during cleanup
            pass
