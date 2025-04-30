"""
Common test fixtures and helper functions for KDIFF tests.
"""

import os
import tempfile
import shutil
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, List, Iterator, Any, Tuple

from kdiff.core.reader import KafkaMessage, MessageMeta
from kdiff.core.config import KDiffConfig


@pytest.fixture
def temp_config_dir():
    """Create a temporary config directory for tests."""
    temp_dir = tempfile.mkdtemp(prefix="kdiff_test_")
    
    # Set up required subdirectories
    os.makedirs(os.path.join(temp_dir, "clusters"), exist_ok=True)
    
    yield temp_dir
    
    # Clean up after tests
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_config():
    """Create a mock configuration object."""
    config = MagicMock(spec=KDiffConfig)
    config.config_dir = "/tmp/kdiff_test"
    config.get_default_cluster.return_value = "default-cluster"
    config.get_available_clusters.return_value = ["default-cluster", "test-cluster"]
    
    return config


@pytest.fixture
def create_test_message():
    """Fixture providing a function to create test messages."""
    def _create_message(content: bytes, offset: int, timestamp: int, partition: int = 0,
                        key: bytes = None, schema_id: int = None) -> KafkaMessage:
        meta = MessageMeta(
            offset=offset,
            timestamp=timestamp,
            partition=partition,
            key=key,
            schema_id=schema_id
        )
        return KafkaMessage(content=content, meta=meta)
    
    return _create_message


@pytest.fixture
def sample_messages(create_test_message):
    """Create a set of sample messages for testing."""
    messages = {
        "msg1": create_test_message(b"test message 1", 0, 1000, key=b"key1"),
        "msg2": create_test_message(b"test message 2", 1, 2000, key=b"key2"),
        "msg3": create_test_message(b"test message 3", 2, 3000, key=b"key3"),
        "msg_dup": create_test_message(b"test message 1", 3, 4000, key=b"key1"),  # Duplicate content of msg1
        "msg_schema": create_test_message(b"schema test", 4, 5000, key=b"key4", schema_id=42),
    }
    
    return messages


@pytest.fixture
def mock_kafka_admin_client():
    """Create a mock Kafka AdminClient."""
    admin_client = MagicMock()
    
    # Mock list_topics method
    admin_client.list_topics.return_value.topics = {
        "test-topic-1": MagicMock(),
        "test-topic-2": MagicMock(),
        "test-topic-3": MagicMock()
    }
    
    return admin_client


@pytest.fixture
def mock_kafka_consumer():
    """Create a mock Kafka Consumer."""
    consumer = MagicMock()
    
    # Set up behavior for poll method
    def mock_poll(timeout):
        # This would be extended in specific tests to return appropriate messages
        return []
    
    consumer.poll.side_effect = mock_poll
    
    return consumer


@pytest.fixture
def integration_kafka_setup():
    """
    Set up a Kafka environment for integration tests.
    
    This is a placeholder fixture. In a real setup, this would:
    1. Start Docker containers for Kafka and ZooKeeper
    2. Create test topics and populate with data
    3. Provide connection details
    
    For now, we'll just mock this functionality.
    """
    # Mock setup would typically involve starting containers
    
    # Create connection details for tests
    kafka_details = {
        "bootstrap_servers": "localhost:9092",
        "topics": ["test-topic-1", "test-topic-2"],
        "client_id": "kdiff-test-client"
    }
    
    yield kafka_details
    
    # Mock teardown would typically involve stopping containers


@pytest.fixture
def cli_runner():
    """Set up CLI test runner with isolated file system."""
    from click.testing import CliRunner
    
    runner = CliRunner()
    with runner.isolated_filesystem():
        yield runner
