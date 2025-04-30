"""
Comparator module for KDIFF.

This module provides functionality to compare messages between Kafka topics
using different comparison strategies.
"""

import logging
import hashlib
import time
import collections
from typing import Dict, Any, Optional, List, Iterator, Tuple, Set, Callable, DefaultDict
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from kdiff.core.reader import KafkaMessage, MessageMeta, TopicReader

logger = logging.getLogger(__name__)


class ComparisonError(Exception):
    """Exception raised for comparison errors."""
    pass


class ChecksumAlgorithm(Enum):
    """Supported checksum algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"


@dataclass
class ComparisonOptions:
    """Options for message comparison."""
    
    compare_content: bool = True
    compare_offsets: bool = False
    compare_timestamps: bool = False
    compare_schema_ids: bool = False
    checksum_algorithm: Optional[ChecksumAlgorithm] = None
    max_differences: Optional[int] = None
    timestamp_tolerance_ms: int = 0  # Tolerance for timestamp differences in milliseconds


@dataclass
class MessageDifference:
    """Represents a difference between two messages."""
    
    index: int  # Position in the stream
    topic_a: str
    topic_b: str
    message_a: Optional[KafkaMessage]
    message_b: Optional[KafkaMessage]
    difference_type: str  # "content", "offset", "timestamp", "schema_id", "missing"
    details: str
    
    @property
    def is_missing(self) -> bool:
        """Check if this difference is due to a missing message."""
        return self.message_a is None or self.message_b is None


@dataclass
class ComparisonResult:
    """Result of a comparison between two message streams."""
    
    topic_a: str
    topic_b: str
    messages_compared: int
    differences_found: int
    missing_in_a: int
    missing_in_b: int
    content_differences: int
    offset_differences: int
    timestamp_differences: int
    schema_id_differences: int
    differences: List[MessageDifference]
    start_time: float
    end_time: float
    options: ComparisonOptions
    
    @property
    def elapsed_seconds(self) -> float:
        """Get the elapsed time in seconds."""
        return self.end_time - self.start_time
    
    @property
    def messages_per_second(self) -> float:
        """Get the messages compared per second."""
        if self.elapsed_seconds == 0:
            return 0.0
        return self.messages_compared / self.elapsed_seconds
    
    @property
    def has_differences(self) -> bool:
        """Check if any differences were found."""
        return self.differences_found > 0
    
    def __str__(self) -> str:
        """Get a string representation of the result."""
        result = [
            f"Comparison Results: {self.topic_a} vs {self.topic_b}",
            f"Messages compared: {self.messages_compared}",
            f"Differences found: {self.differences_found}",
            f"  - Missing in {self.topic_a}: {self.missing_in_a}",
            f"  - Missing in {self.topic_b}: {self.missing_in_b}",
            f"  - Content differences: {self.content_differences}",
        ]
        
        if self.options.compare_offsets:
            result.append(f"  - Offset differences: {self.offset_differences}")
            
        if self.options.compare_timestamps:
            result.append(f"  - Timestamp differences: {self.timestamp_differences}")
            
        if self.options.compare_schema_ids:
            result.append(f"  - Schema ID differences: {self.schema_id_differences}")
            
        result.append(f"Elapsed time: {self.elapsed_seconds:.2f} seconds")
        result.append(f"Processing rate: {self.messages_per_second:.2f} messages/second")
        
        return "\n".join(result)


@dataclass
class ComparisonProgress:
    """Progress information for a comparison operation."""
    
    topic_a: str
    topic_b: str
    messages_compared: int
    differences_found: int
    start_time: float
    max_messages: Optional[int] = None
    
    @property
    def percentage_complete(self) -> float:
        """
        Get the percentage of completion.
        
        Returns:
            Percentage complete (0-100)
        """
        if self.max_messages is None:
            return 0.0
        
        return min(100.0, (self.messages_compared / self.max_messages) * 100.0)
    
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
        Get the messages compared per second.
        
        Returns:
            Messages per second
        """
        if self.elapsed_seconds == 0:
            return 0.0
        
        return self.messages_compared / self.elapsed_seconds
    
    def __str__(self) -> str:
        """
        Get a string representation of the progress.
        
        Returns:
            String representation
        """
        if self.max_messages is not None:
            percentage = f"{self.percentage_complete:.1f}%"
            messages = f"{self.messages_compared}/{self.max_messages} messages"
        else:
            percentage = "In progress"
            messages = f"{self.messages_compared} messages"
        
        differences = f"{self.differences_found} differences"
        rate = f"{self.messages_per_second:.1f} msg/s"
        elapsed = f"{self.elapsed_seconds:.1f}s elapsed"
        
        return f"{percentage} - {messages} - {differences} - {rate} - {elapsed}"


class StreamComparator:
    """
    Comparator for Kafka message streams.
    
    This class handles comparing messages between two Kafka topics
    treating them as ordered streams where sequence matters.
    """
    
    def __init__(self, options: Optional[ComparisonOptions] = None):
        """
        Initialize the stream comparator.
        
        Args:
            options: Comparison options
        """
        self.options = options or ComparisonOptions()
        self.progress_callback: Optional[Callable[[ComparisonProgress], None]] = None
    
    def set_progress_callback(self, callback: Callable[[ComparisonProgress], None]):
        """
        Set a callback function to report progress.
        
        Args:
            callback: Function to call with progress updates
        """
        self.progress_callback = callback
    
    def _calculate_checksum(self, data: bytes) -> str:
        """
        Calculate a checksum for the given data.
        
        Args:
            data: The data to calculate the checksum for
            
        Returns:
            Checksum string
        """
        if self.options.checksum_algorithm == ChecksumAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif self.options.checksum_algorithm == ChecksumAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif self.options.checksum_algorithm == ChecksumAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        else:
            # Default to SHA256
            return hashlib.sha256(data).hexdigest()
    
    def _compare_content(self, message_a: KafkaMessage, message_b: KafkaMessage) -> Tuple[bool, str]:
        """
        Compare the content of two messages.
        
        Args:
            message_a: First message
            message_b: Second message
            
        Returns:
            Tuple of (is_equal, details)
        """
        if self.options.checksum_algorithm:
            # Compare using checksums
            checksum_a = self._calculate_checksum(message_a.content)
            checksum_b = self._calculate_checksum(message_b.content)
            
            if checksum_a != checksum_b:
                return False, f"Checksum mismatch: {checksum_a} != {checksum_b}"
            return True, ""
        else:
            # Direct binary comparison
            if message_a.content != message_b.content:
                # For binary data, just report the length and first few bytes
                preview_a = message_a.content[:20].hex() if message_a.content else "None"
                preview_b = message_b.content[:20].hex() if message_b.content else "None"
                
                return False, f"Content mismatch: {len(message_a.content)} bytes vs {len(message_b.content)} bytes. First bytes: {preview_a}... vs {preview_b}..."
            return True, ""
    
    def _compare_offsets(self, meta_a: MessageMeta, meta_b: MessageMeta) -> Tuple[bool, str]:
        """
        Compare the offsets of two messages.
        
        Args:
            meta_a: First message metadata
            meta_b: Second message metadata
            
        Returns:
            Tuple of (is_equal, details)
        """
        if meta_a.offset != meta_b.offset:
            return False, f"Offset mismatch: {meta_a.offset} != {meta_b.offset}"
        return True, ""
    
    def _compare_timestamps(self, meta_a: MessageMeta, meta_b: MessageMeta) -> Tuple[bool, str]:
        """
        Compare the timestamps of two messages.
        
        Args:
            meta_a: First message metadata
            meta_b: Second message metadata
            
        Returns:
            Tuple of (is_equal, details)
        """
        # Apply tolerance if specified
        if abs(meta_a.timestamp - meta_b.timestamp) > self.options.timestamp_tolerance_ms:
            return False, f"Timestamp mismatch: {meta_a.timestamp} != {meta_b.timestamp} (tolerance: {self.options.timestamp_tolerance_ms}ms)"
        return True, ""
    
    def _compare_schema_ids(self, meta_a: MessageMeta, meta_b: MessageMeta) -> Tuple[bool, str]:
        """
        Compare the schema IDs of two messages.
        
        Args:
            meta_a: First message metadata
            meta_b: Second message metadata
            
        Returns:
            Tuple of (is_equal, details)
        """
        if meta_a.schema_id != meta_b.schema_id:
            return False, f"Schema ID mismatch: {meta_a.schema_id} != {meta_b.schema_id}"
        return True, ""
    
    def _compare_messages(self, index: int, message_a: KafkaMessage, message_b: KafkaMessage, 
                         topic_a: str, topic_b: str) -> List[MessageDifference]:
        """
        Compare two messages and return any differences.
        
        Args:
            index: Position in the stream
            message_a: First message
            message_b: Second message
            topic_a: Name of the first topic
            topic_b: Name of the second topic
            
        Returns:
            List of differences found
        """
        differences = []
        
        # Compare content if enabled
        if self.options.compare_content:
            is_equal, details = self._compare_content(message_a, message_b)
            if not is_equal:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="content",
                    details=details
                ))
        
        # Compare offsets if enabled
        if self.options.compare_offsets:
            is_equal, details = self._compare_offsets(message_a.meta, message_b.meta)
            if not is_equal:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="offset",
                    details=details
                ))
        
        # Compare timestamps if enabled
        if self.options.compare_timestamps:
            is_equal, details = self._compare_timestamps(message_a.meta, message_b.meta)
            if not is_equal:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="timestamp",
                    details=details
                ))
        
        # Compare schema IDs if enabled
        if self.options.compare_schema_ids:
            is_equal, details = self._compare_schema_ids(message_a.meta, message_b.meta)
            if not is_equal:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="schema_id",
                    details=details
                ))
        
        return differences
    
    def compare(self, topic_a: str, messages_a: Iterator[KafkaMessage],
                topic_b: str, messages_b: Iterator[KafkaMessage],
                options: Optional[ComparisonOptions] = None) -> ComparisonResult:
        """
        Compare two streams of messages.
        
        Args:
            topic_a: Name of the first topic
            messages_a: Messages from the first topic
            topic_b: Name of the second topic
            messages_b: Messages from the second topic
            options: Override the instance's comparison options
            
        Returns:
            Comparison result
        """
        if options:
            self.options = options
            
        return self.compare_streams(
            stream_a=messages_a,
            stream_b=messages_b,
            topic_a=topic_a,
            topic_b=topic_b,
            max_messages=None
        )
        
    def compare_streams(self, 
                       stream_a: Iterator[KafkaMessage], 
                       stream_b: Iterator[KafkaMessage],
                       topic_a: str,
                       topic_b: str,
                       max_messages: Optional[int] = None) -> ComparisonResult:
        """
        Compare two message streams.
        
        Args:
            stream_a: First message stream
            stream_b: Second message stream
            topic_a: Name of the first topic
            topic_b: Name of the second topic
            max_messages: Maximum number of messages to compare
            
        Returns:
            Comparison result
        """
        start_time = time.time()
        
        # Initialize result counters
        messages_compared = 0
        differences_found = 0
        missing_in_a = 0
        missing_in_b = 0
        content_differences = 0
        offset_differences = 0
        timestamp_differences = 0
        schema_id_differences = 0
        differences = []
        
        # Set up progress tracking
        progress = ComparisonProgress(
            topic_a=topic_a,
            topic_b=topic_b,
            messages_compared=0,
            differences_found=0,
            start_time=start_time,
            max_messages=max_messages
        )
        
        # Compare messages
        try:
            # Get first messages from each stream
            message_a = next(stream_a, None)
            message_b = next(stream_b, None)
            
            index = 0
            
            while (message_a is not None or message_b is not None) and (max_messages is None or messages_compared < max_messages):
                # Check for missing messages
                if message_a is None:
                    # Message missing in stream A
                    differences.append(MessageDifference(
                        index=index,
                        topic_a=topic_a,
                        topic_b=topic_b,
                        message_a=None,
                        message_b=message_b,
                        difference_type="missing",
                        details=f"Message missing in {topic_a}"
                    ))
                    
                    missing_in_a += 1
                    differences_found += 1
                    
                    # Move to next message in stream B
                    message_b = next(stream_b, None)
                elif message_b is None:
                    # Message missing in stream B
                    differences.append(MessageDifference(
                        index=index,
                        topic_a=topic_a,
                        topic_b=topic_b,
                        message_a=message_a,
                        message_b=None,
                        difference_type="missing",
                        details=f"Message missing in {topic_b}"
                    ))
                    
                    missing_in_b += 1
                    differences_found += 1
                    
                    # Move to next message in stream A
                    message_a = next(stream_a, None)
                else:
                    # Both messages exist, compare them
                    message_differences = self._compare_messages(
                        index, message_a, message_b, topic_a, topic_b
                    )
                    
                    # Update counters
                    if message_differences:
                        differences.extend(message_differences)
                        differences_found += len(message_differences)
                        
                        for diff in message_differences:
                            if diff.difference_type == "content":
                                content_differences += 1
                            elif diff.difference_type == "offset":
                                offset_differences += 1
                            elif diff.difference_type == "timestamp":
                                timestamp_differences += 1
                            elif diff.difference_type == "schema_id":
                                schema_id_differences += 1
                    
                    # Move to next messages
                    message_a = next(stream_a, None)
                    message_b = next(stream_b, None)
                
                # Update counters
                messages_compared += 1
                index += 1
                
                # Update progress
                progress.messages_compared = messages_compared
                progress.differences_found = differences_found
                
                # Report progress periodically
                if self.progress_callback and messages_compared % 1000 == 0:
                    self.progress_callback(progress)
                
                # Check if we've reached the maximum differences and should stop processing
                if self.options.max_differences is not None and differences_found >= self.options.max_differences:
                    logger.info(f"Reached maximum differences limit: {self.options.max_differences}")
                    # Stop processing more differences
                    break
        except Exception as e:
            logger.exception(f"Error comparing streams: {e}")
            raise ComparisonError(f"Failed to compare streams: {e}")
        
        # Final progress update
        if self.progress_callback:
            self.progress_callback(progress)
        
        # Create result
        end_time = time.time()
        result = ComparisonResult(
            topic_a=topic_a,
            topic_b=topic_b,
            messages_compared=messages_compared,
            differences_found=differences_found,
            missing_in_a=missing_in_a,
            missing_in_b=missing_in_b,
            content_differences=content_differences,
            offset_differences=offset_differences,
            timestamp_differences=timestamp_differences,
            schema_id_differences=schema_id_differences,
            differences=differences,
            start_time=start_time,
            end_time=end_time,
            options=self.options
        )
        
        return result
    
    def compare_topics(self, 
                      reader: TopicReader,
                      cluster_a: str,
                      topic_a: str,
                      cluster_b: str,
                      topic_b: str,
                      start_offset_a: Optional[int] = None,
                      end_offset_a: Optional[int] = None,
                      start_offset_b: Optional[int] = None,
                      end_offset_b: Optional[int] = None,
                      start_timestamp_a: Optional[int] = None,
                      end_timestamp_a: Optional[int] = None,
                      start_timestamp_b: Optional[int] = None,
                      end_timestamp_b: Optional[int] = None,
                      max_messages: Optional[int] = None) -> ComparisonResult:
        """
        Compare two Kafka topics.
        
        Args:
            reader: Topic reader
            cluster_a: Name of the first cluster
            topic_a: Name of the first topic
            cluster_b: Name of the second cluster
            topic_b: Name of the second topic
            start_offset_a: Start offset for topic A
            end_offset_a: End offset for topic A
            start_offset_b: Start offset for topic B
            end_offset_b: End offset for topic B
            start_timestamp_a: Start timestamp for topic A
            end_timestamp_a: End timestamp for topic A
            start_timestamp_b: Start timestamp for topic B
            end_timestamp_b: End timestamp for topic B
            max_messages: Maximum number of messages to compare
            
        Returns:
            Comparison result
        """
        try:
            # Create streams for both topics
            stream_a = reader.read_topic(
                cluster_name=cluster_a,
                topic=topic_a,
                start_offset=start_offset_a,
                end_offset=end_offset_a,
                start_timestamp=start_timestamp_a,
                end_timestamp=end_timestamp_a,
                max_messages=max_messages
            )
            
            stream_b = reader.read_topic(
                cluster_name=cluster_b,
                topic=topic_b,
                start_offset=start_offset_b,
                end_offset=end_offset_b,
                start_timestamp=start_timestamp_b,
                end_timestamp=end_timestamp_b,
                max_messages=max_messages
            )
            
            # Compare the streams
            return self.compare_streams(
                stream_a=stream_a,
                stream_b=stream_b,
                topic_a=topic_a,
                topic_b=topic_b,
                max_messages=max_messages
            )
        except Exception as e:
            logger.exception(f"Error comparing topics: {e}")
            raise ComparisonError(f"Failed to compare topics: {e}")


class TableComparator:
    """
    Comparator for Kafka message collections.
    
    This class handles comparing messages between two Kafka topics
    treating them as unordered collections where only content equality matters,
    not sequence.
    """
    
    def __init__(self, options: Optional[ComparisonOptions] = None):
        """
        Initialize the table comparator.
        
        Args:
            options: Comparison options
        """
        self.options = options or ComparisonOptions()
        self.progress_callback: Optional[Callable[[ComparisonProgress], None]] = None
    
    def set_progress_callback(self, callback: Callable[[ComparisonProgress], None]):
        """
        Set a callback function to report progress.
        
        Args:
            callback: Function to call with progress updates
        """
        self.progress_callback = callback
    
    def _calculate_checksum(self, data: bytes) -> str:
        """
        Calculate a checksum for the given data.
        
        Args:
            data: The data to calculate the checksum for
            
        Returns:
            Checksum string
        """
        if self.options.checksum_algorithm == ChecksumAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif self.options.checksum_algorithm == ChecksumAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif self.options.checksum_algorithm == ChecksumAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        else:
            # Default to SHA256
            return hashlib.sha256(data).hexdigest()
    
    def _get_message_key(self, message: KafkaMessage) -> str:
        """
        Get a unique key for a message to use for comparison.
        
        Args:
            message: The message to get a key for
            
        Returns:
            Key string used for comparison
        """
        # If using checksums, generate a checksum of the content
        if self.options.checksum_algorithm:
            return self._calculate_checksum(message.content)
        else:
            # Otherwise use the content directly (in hex for binary safety)
            return message.content.hex()
    
    def _compare_messages(self, index: int, message_a: KafkaMessage, message_b: KafkaMessage, 
                         topic_a: str, topic_b: str) -> List[MessageDifference]:
        """
        Compare two messages and return any differences.
        
        Args:
            index: Index for tracking purposes
            message_a: First message
            message_b: Second message
            topic_a: Name of the first topic
            topic_b: Name of the second topic
            
        Returns:
            List of differences found
        """
        differences = []
        
        # Compare content if enabled - in table mode, messages with the same key 
        # should have identical content, but we double-check to be safe
        if self.options.compare_content:
            if message_a.content != message_b.content:
                # For binary data, just report the length and first few bytes
                preview_a = message_a.content[:20].hex() if message_a.content else "None"
                preview_b = message_b.content[:20].hex() if message_b.content else "None"
                
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="content",
                    details=f"Content mismatch: {len(message_a.content)} bytes vs {len(message_b.content)} bytes. First bytes: {preview_a}... vs {preview_b}..."
                ))
        
        # Compare additional fields if enabled
        if self.options.compare_offsets:
            if message_a.meta.offset != message_b.meta.offset:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="offset",
                    details=f"Offset mismatch: {message_a.meta.offset} != {message_b.meta.offset}"
                ))
        
        if self.options.compare_timestamps:
            if abs(message_a.meta.timestamp - message_b.meta.timestamp) > self.options.timestamp_tolerance_ms:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="timestamp",
                    details=f"Timestamp mismatch: {message_a.meta.timestamp} != {message_b.meta.timestamp} (tolerance: {self.options.timestamp_tolerance_ms}ms)"
                ))
        
        if self.options.compare_schema_ids:
            if message_a.meta.schema_id != message_b.meta.schema_id:
                differences.append(MessageDifference(
                    index=index,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    message_a=message_a,
                    message_b=message_b,
                    difference_type="schema_id",
                    details=f"Schema ID mismatch: {message_a.meta.schema_id} != {message_b.meta.schema_id}"
                ))
        
        return differences
    
    def compare(self, topic_a: str, messages_a: Iterator[KafkaMessage], 
               topic_b: str, messages_b: Iterator[KafkaMessage], 
               options: Optional[ComparisonOptions] = None) -> ComparisonResult:
        """
        Compare two collections of messages as unordered sets.
        
        Args:
            topic_a: Name of the first topic
            messages_a: Messages from the first topic
            topic_b: Name of the second topic
            messages_b: Messages from the second topic
            options: Override the instance's comparison options
            
        Returns:
            Comparison result
        """
        if options:
            self.options = options
            
        start_time = time.time()
        
        # Initialize result counters
        messages_compared = 0
        differences_found = 0
        missing_in_a = 0
        missing_in_b = 0
        content_differences = 0
        offset_differences = 0
        timestamp_differences = 0
        schema_id_differences = 0
        duplicate_in_a = 0
        duplicate_in_b = 0
        differences = []
        
        # Set up progress tracking
        progress = ComparisonProgress(
            topic_a=topic_a,
            topic_b=topic_b,
            messages_compared=0,
            differences_found=0,
            start_time=start_time
        )
        
        # Track progress periodically
        def update_progress():
            nonlocal messages_compared
            
            if self.progress_callback and messages_compared % 1000 == 0:
                progress.messages_compared = messages_compared
                progress.differences_found = differences_found
                self.progress_callback(progress)
                
        try:
            # First pass: collect all messages and group by content hash
            logger.debug(f"Collecting messages from {topic_a}")
            messages_by_key_a: DefaultDict[str, List[KafkaMessage]] = collections.defaultdict(list)
            for msg in messages_a:
                key = self._get_message_key(msg)
                messages_by_key_a[key].append(msg)
                messages_compared += 1
                update_progress()
            
            logger.debug(f"Collecting messages from {topic_b}")
            messages_by_key_b: DefaultDict[str, List[KafkaMessage]] = collections.defaultdict(list)
            for msg in messages_b:
                key = self._get_message_key(msg)
                messages_by_key_b[key].append(msg)
                messages_compared += 1
                update_progress()
                
            # Find duplicate messages in each topic
            for key, msgs in messages_by_key_a.items():
                if len(msgs) > 1:
                    # We found duplicates based on content in topic A
                    duplicate_in_a += len(msgs) - 1
                    logger.debug(f"Found {len(msgs)} duplicate messages in {topic_a} with key {key}")
            
            for key, msgs in messages_by_key_b.items():
                if len(msgs) > 1:
                    # We found duplicates based on content in topic B
                    duplicate_in_b += len(msgs) - 1
                    logger.debug(f"Found {len(msgs)} duplicate messages in {topic_b} with key {key}")
            
            # Find messages missing in each topic
            index = 0
            unique_keys_a = set(messages_by_key_a.keys())
            unique_keys_b = set(messages_by_key_b.keys())
            
            # Messages in A but not in B
            for key in unique_keys_a - unique_keys_b:
                for msg in messages_by_key_a[key]:
                    differences.append(MessageDifference(
                        index=index,
                        topic_a=topic_a,
                        topic_b=topic_b,
                        message_a=msg,
                        message_b=None,
                        difference_type="missing",
                        details=f"Message missing in {topic_b}"
                    ))
                    index += 1
                    missing_in_b += 1
                    differences_found += 1
            
            # Messages in B but not in A
            for key in unique_keys_b - unique_keys_a:
                for msg in messages_by_key_b[key]:
                    differences.append(MessageDifference(
                        index=index,
                        topic_a=topic_a,
                        topic_b=topic_b,
                        message_a=None,
                        message_b=msg,
                        difference_type="missing",
                        details=f"Message missing in {topic_a}"
                    ))
                    index += 1
                    missing_in_a += 1
                    differences_found += 1
            
            # Compare messages that exist in both topics
            for key in unique_keys_a & unique_keys_b:
                # For each key, compare the first message from each collection
                # (Other instances are duplicates and have already been counted)
                message_a = messages_by_key_a[key][0]
                message_b = messages_by_key_b[key][0]
                
                # Compare messages for metadata differences
                message_differences = self._compare_messages(
                    index, message_a, message_b, topic_a, topic_b
                )
                
                if message_differences:
                    differences.extend(message_differences)
                    differences_found += len(message_differences)
                    
                    for diff in message_differences:
                        if diff.difference_type == "content":
                            content_differences += 1
                        elif diff.difference_type == "offset":
                            offset_differences += 1
                        elif diff.difference_type == "timestamp":
                            timestamp_differences += 1
                        elif diff.difference_type == "schema_id":
                            schema_id_differences += 1
                
                index += 1
                
            # If we've reached maximum differences, cut the differences list
            if self.options.max_differences is not None and differences_found > self.options.max_differences:
                logger.info(f"Reached maximum differences limit: {self.options.max_differences}")
                # Truncate the differences list to max_differences
                differences = differences[:self.options.max_differences]
                differences_found = self.options.max_differences
                
        except Exception as e:
            logger.exception(f"Error comparing message collections: {e}")
            raise ComparisonError(f"Failed to compare message collections: {e}")
        
        # Final progress update
        if self.progress_callback:
            progress.messages_compared = messages_compared
            progress.differences_found = differences_found
            self.progress_callback(progress)
        
        # Add a note about duplicates to the differences list
        if duplicate_in_a > 0:
            differences.append(MessageDifference(
                index=-1,  # Using -1 to indicate a special index for duplicates
                topic_a=topic_a,
                topic_b=topic_b,
                message_a=None,
                message_b=None,
                difference_type="duplicate",
                details=f"Found {duplicate_in_a} duplicate messages in {topic_a}"
            ))
        
        if duplicate_in_b > 0:
            differences.append(MessageDifference(
                index=-1,  # Using -1 to indicate a special index for duplicates
                topic_a=topic_a,
                topic_b=topic_b,
                message_a=None,
                message_b=None,
                difference_type="duplicate",
                details=f"Found {duplicate_in_b} duplicate messages in {topic_b}"
            ))
        
        # Create result
        end_time = time.time()
        result = ComparisonResult(
            topic_a=topic_a,
            topic_b=topic_b,
            messages_compared=messages_compared,
            differences_found=differences_found,
            missing_in_a=missing_in_a,
            missing_in_b=missing_in_b,
            content_differences=content_differences,
            offset_differences=offset_differences,
            timestamp_differences=timestamp_differences,
            schema_id_differences=schema_id_differences,
            differences=differences,
            start_time=start_time,
            end_time=end_time,
            options=self.options
        )
        
        return result
