"""
Unit tests for the KDIFF Kafka connector.
"""

import unittest
from unittest.mock import MagicMock, patch, call
import time

from kafka.errors import NoBrokersAvailable

from kdiff.core.config import KDiffConfig
from kdiff.core.connector import KafkaConnector, KafkaConnectionError


class TestKafkaConnector(unittest.TestCase):
    """Tests for the KafkaConnector class."""
    
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
        
        # Set up mock to return the test cluster properties
        self.config.get_cluster_config.return_value = self.test_cluster_props
        
        # Create connector with the mock config
        self.connector = KafkaConnector(self.config)
    
    @patch('kdiff.core.connector.KafkaConsumer')
    def test_connect_consumer(self, mock_consumer):
        """Test connecting a consumer client."""
        # Set up the mock
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.topics.return_value = ["topic1", "topic2"]
        mock_consumer.return_value = mock_consumer_instance
        
        # Call connect
        client = self.connector.connect("test-cluster", "consumer")
        
        # Check the result
        self.assertEqual(client, mock_consumer_instance)
        self.config.get_cluster_config.assert_called_once_with("test-cluster")
        
        # Check that the connection was stored
        self.assertIn("test-cluster_consumer", self.connector.cluster_connections)
        self.assertEqual(
            self.connector.cluster_connections["test-cluster_consumer"]["client"],
            mock_consumer_instance
        )
        
        # Check consumer was created with correct config
        expected_config = {
            "bootstrap_servers": "test-broker:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test-user",
            "sasl_plain_password": "test-password",
            "client_id": "test-client",
            "session_timeout_ms": 45000
        }
        mock_consumer.assert_called_once_with(**expected_config)
        
        # Call connect again, should reuse the connection
        self.connector.connect("test-cluster", "consumer")
        
        # Should still be called only once (connection reused)
        mock_consumer.assert_called_once()
    
    @patch('kdiff.core.connector.KafkaProducer')
    def test_connect_producer(self, mock_producer):
        """Test connecting a producer client."""
        # Set up the mock
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        
        # Call connect
        client = self.connector.connect("test-cluster", "producer")
        
        # Check the result
        self.assertEqual(client, mock_producer_instance)
        self.config.get_cluster_config.assert_called_once_with("test-cluster")
        
        # Check that the connection was stored
        self.assertIn("test-cluster_producer", self.connector.cluster_connections)
        self.assertEqual(
            self.connector.cluster_connections["test-cluster_producer"]["client"],
            mock_producer_instance
        )
    
    @patch('kdiff.core.connector.KafkaAdminClient')
    def test_connect_admin(self, mock_admin):
        """Test connecting an admin client."""
        # Set up the mock
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance
        
        # Call connect
        client = self.connector.connect("test-cluster", "admin")
        
        # Check the result
        self.assertEqual(client, mock_admin_instance)
        self.config.get_cluster_config.assert_called_once_with("test-cluster")
        
        # Check that the connection was stored
        self.assertIn("test-cluster_admin", self.connector.cluster_connections)
        self.assertEqual(
            self.connector.cluster_connections["test-cluster_admin"]["client"],
            mock_admin_instance
        )
    
    @patch('kdiff.core.connector.KafkaConsumer')
    @patch('time.sleep')  # Mock sleep to avoid waiting in tests
    def test_connection_retry(self, mock_sleep, mock_consumer):
        """Test connection retry logic."""
        # Set up the mock to fail twice and succeed on third attempt
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.topics.return_value = ["topic1", "topic2"]
        mock_consumer.side_effect = [
            NoBrokersAvailable("No brokers available"),
            NoBrokersAvailable("No brokers available"),
            mock_consumer_instance
        ]
        
        # Call connect
        client = self.connector.connect("test-cluster", "consumer")
        
        # Check the result
        self.assertEqual(client, mock_consumer_instance)
        
        # Should have been called 3 times
        self.assertEqual(mock_consumer.call_count, 3)
        
        # Should have slept twice (after first and second failures)
        self.assertEqual(mock_sleep.call_count, 2)
        
        # Check that sleep times follow exponential backoff
        expected_sleep_calls = [call(2), call(4)]  # 2^1, 2^2
        mock_sleep.assert_has_calls(expected_sleep_calls)
    
    @patch('kdiff.core.connector.KafkaConsumer')
    @patch('time.sleep')
    def test_connection_failure_after_retries(self, mock_sleep, mock_consumer):
        """Test connection failure after maximum retries."""
        # Set up the mock to always fail
        mock_consumer.side_effect = NoBrokersAvailable("No brokers available")
        
        # Call connect, should raise exception after retries
        with self.assertRaises(KafkaConnectionError) as context:
            self.connector.connect("test-cluster", "consumer")
        
        # Check error message
        self.assertIn("Failed to connect to cluster test-cluster after 3 attempts", str(context.exception))
        
        # Should have been called 3 times (max retries)
        self.assertEqual(mock_consumer.call_count, 3)
        
        # Should have slept 3 times (after each failure)
        self.assertEqual(mock_sleep.call_count, 3)
    
    @patch('kdiff.core.connector.KafkaConsumer')
    def test_list_topics(self, mock_consumer):
        """Test listing topics."""
        # Set up the mock
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.topics.return_value = {"topic1", "topic2", "topic3"}
        mock_consumer.return_value = mock_consumer_instance
        
        # Call list_topics
        topics = self.connector.list_topics("test-cluster")
        
        # Check the result
        self.assertEqual(topics, ["topic1", "topic2", "topic3"])
        
        # Should have created a consumer
        mock_consumer.assert_called_once()
    
    @patch('kdiff.core.connector.KafkaConsumer')
    def test_get_consumer(self, mock_consumer):
        """Test getting a consumer with specific topics."""
        # Set up the mock
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
        # Call get_consumer with topics
        consumer = self.connector.get_consumer("test-cluster", topics=["topic1", "topic2"])
        
        # Check the result
        self.assertEqual(consumer, mock_consumer_instance)
        
        # Should have created a consumer with the topics
        mock_consumer.assert_called_once()
        args, _ = mock_consumer.call_args
        self.assertEqual(args, ("topic1", "topic2"))
    
    @patch('kdiff.core.connector.KafkaConsumer')
    def test_get_consumer_with_additional_params(self, mock_consumer):
        """Test getting a consumer with additional parameters."""
        # Set up the mock
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        
        # Call get_consumer with additional parameters
        consumer = self.connector.get_consumer(
            "test-cluster",
            auto_offset_reset="earliest",
            group_id="test-group"
        )
        
        # Check the result
        self.assertEqual(consumer, mock_consumer_instance)
        
        # Should have created a consumer with the additional parameters
        mock_consumer.assert_called_once()
        _, kwargs = mock_consumer.call_args
        self.assertEqual(kwargs["auto_offset_reset"], "earliest")
        self.assertEqual(kwargs["group_id"], "test-group")
    
    @patch('kdiff.core.connector.KafkaAdminClient')
    def test_get_admin_client(self, mock_admin):
        """Test getting an admin client."""
        # Set up the mock
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance
        
        # Call get_admin_client
        admin = self.connector.get_admin_client("test-cluster")
        
        # Check the result
        self.assertEqual(admin, mock_admin_instance)
        
        # Should have created an admin client
        mock_admin.assert_called_once()
    
    @patch('kdiff.core.connector.KafkaConsumer')
    def test_close_connection(self, mock_consumer):
        """Test closing a specific connection."""
        # Set up mocks
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.topics.return_value = ["topic1", "topic2"]
        mock_consumer.return_value = mock_consumer_instance
        
        # Create a connection
        self.connector.connect("test-cluster", "consumer")
        
        # Close the connection
        self.connector.close_connection("test-cluster", "consumer")
        
        # Should have closed the client
        mock_consumer_instance.close.assert_called_once()
        
        # Connection should be removed
        self.assertNotIn("test-cluster_consumer", self.connector.cluster_connections)
        self.assertNotIn("test-cluster_consumer", self.connector.active_connections)
    
    @patch('kdiff.core.connector.KafkaConsumer')
    @patch('kdiff.core.connector.KafkaProducer')
    def test_close_all_connections(self, mock_producer, mock_consumer):
        """Test closing all connections."""
        # Set up mocks
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.topics.return_value = ["topic1", "topic2"]
        mock_consumer.return_value = mock_consumer_instance
        
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        
        # Create connections
        self.connector.connect("test-cluster", "consumer")
        self.connector.connect("test-cluster", "producer")
        
        # Close all connections
        self.connector.close_all_connections()
        
        # Should have closed both clients
        mock_consumer_instance.close.assert_called_once()
        mock_producer_instance.close.assert_called_once()
        
        # All connections should be removed
        self.assertEqual(len(self.connector.cluster_connections), 0)
        self.assertEqual(len(self.connector.active_connections), 0)
    
    def test_create_kafka_config(self):
        """Test creation of Kafka client configuration from properties."""
        # Test with SSL properties
        ssl_props = self.test_cluster_props.copy()
        ssl_props.update({
            "ssl.truststore.location": "/path/to/truststore",
            "ssl.keystore.location": "/path/to/keystore",
            "ssl.keystore.password": "keystore-password"
        })
        
        kafka_config = self.connector._create_kafka_config(ssl_props)
        
        # Check the result
        self.assertEqual(kafka_config["bootstrap_servers"], "test-broker:9092")
        self.assertEqual(kafka_config["security_protocol"], "SASL_SSL")
        self.assertEqual(kafka_config["sasl_mechanism"], "PLAIN")
        self.assertEqual(kafka_config["sasl_plain_username"], "test-user")
        self.assertEqual(kafka_config["sasl_plain_password"], "test-password")
        self.assertEqual(kafka_config["client_id"], "test-client")
        self.assertEqual(kafka_config["session_timeout_ms"], 45000)
        self.assertEqual(kafka_config["ssl_cafile"], "/path/to/truststore")
        self.assertEqual(kafka_config["ssl_certfile"], "/path/to/keystore")
        self.assertEqual(kafka_config["ssl_password"], "keystore-password")
        
        # Test with invalid session timeout
        invalid_props = self.test_cluster_props.copy()
        invalid_props["session.timeout.ms"] = "invalid"
        
        kafka_config = self.connector._create_kafka_config(invalid_props)
        
        # Should not have session_timeout_ms
        self.assertNotIn("session_timeout_ms", kafka_config)


if __name__ == '__main__':
    unittest.main()
