"""
Kafka connector module for KDIFF.

This module provides functionality to establish connections to Kafka clusters
and manage client sessions.
"""

import logging
import time
from typing import Dict, Any, Optional, List, Set
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable

from kdiff.core.config import KDiffConfig, ConfigError
from kdiff.core.cluster_manager import ClusterManager

logger = logging.getLogger(__name__)


class KafkaConnectionError(Exception):
    """Exception raised for Kafka connection errors."""
    pass


class KafkaConnector:
    """
    Manager for Kafka connections.
    
    This class handles establishing and maintaining connections to Kafka clusters.
    It provides connection pooling and reconnection logic.
    """
    
    def __init__(self, config: KDiffConfig):
        """
        Initialize the Kafka connector.
        
        Args:
            config: The KDiff configuration
        """
        self.config = config
        self.cluster_connections: Dict[str, Dict[str, Any]] = {}
        self.cluster_properties: Dict[str, Dict[str, str]] = {}
        self.active_connections: Set[str] = set()
        
        # Create thread pool for connection operations
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def _get_cluster_config(self, cluster_name: str) -> Dict[str, str]:
        """
        Get configuration for a specific cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Dictionary of cluster configuration properties
            
        Raises:
            KafkaConnectionError: If the cluster configuration is not found
        """
        try:
            if cluster_name in self.cluster_properties:
                return self.cluster_properties[cluster_name]
                
            props = self.config.get_cluster_config(cluster_name)
            self.cluster_properties[cluster_name] = props
            return props
        except ConfigError as e:
            raise KafkaConnectionError(f"Failed to get cluster configuration: {e}")
    
    def _create_kafka_config(self, cluster_props: Dict[str, str]) -> Dict[str, Any]:
        """
        Create Kafka client configuration from properties.
        
        Args:
            cluster_props: Cluster properties from client.properties
            
        Returns:
            Dictionary of Kafka client configuration
        """
        kafka_config = {}
        
        # Required properties
        if "bootstrap.servers" in cluster_props:
            kafka_config["bootstrap_servers"] = cluster_props["bootstrap.servers"]
        
        # Security protocol
        if "security.protocol" in cluster_props:
            kafka_config["security_protocol"] = cluster_props["security.protocol"]
            
            # SASL configuration
            if cluster_props["security.protocol"] in ["SASL_PLAINTEXT", "SASL_SSL"]:
                if "sasl.mechanisms" in cluster_props:
                    kafka_config["sasl_mechanism"] = cluster_props["sasl.mechanisms"]
                
                if "sasl.username" in cluster_props and "sasl.password" in cluster_props:
                    kafka_config["sasl_plain_username"] = cluster_props["sasl.username"]
                    kafka_config["sasl_plain_password"] = cluster_props["sasl.password"]
        
        # SSL configuration if needed
        if "security.protocol" in cluster_props and cluster_props["security.protocol"] in ["SSL", "SASL_SSL"]:
            if "ssl.truststore.location" in cluster_props:
                kafka_config["ssl_cafile"] = cluster_props["ssl.truststore.location"]
            
            if "ssl.keystore.location" in cluster_props:
                kafka_config["ssl_certfile"] = cluster_props["ssl.keystore.location"]
                
            if "ssl.keystore.password" in cluster_props:
                kafka_config["ssl_password"] = cluster_props["ssl.keystore.password"]
        
        # Other commonly used properties
        if "client.id" in cluster_props:
            kafka_config["client_id"] = cluster_props["client.id"]
            
        if "group.id" in cluster_props:
            kafka_config["group_id"] = cluster_props["group.id"]
        
        # Session timeout
        if "session.timeout.ms" in cluster_props:
            try:
                kafka_config["session_timeout_ms"] = int(cluster_props["session.timeout.ms"])
            except ValueError:
                logger.warning(f"Invalid session.timeout.ms value: {cluster_props['session.timeout.ms']}")
        
        return kafka_config
    
    def _try_connect(self, cluster_name: str, kafka_config: Dict[str, Any], 
                    client_type: str, max_retries: int = 3) -> Any:
        """
        Try to establish a connection to a Kafka cluster with retries.
        
        Args:
            cluster_name: Name of the cluster
            kafka_config: Kafka client configuration
            client_type: Type of client ("consumer", "producer", "admin")
            max_retries: Maximum number of connection attempts
            
        Returns:
            Kafka client instance
            
        Raises:
            KafkaConnectionError: If connection fails after retries
        """
        retries = 0
        last_error = None
        
        while retries < max_retries:
            try:
                if client_type == "consumer":
                    # Create a consumer without subscribing to any topics yet
                    client = KafkaConsumer(**kafka_config)
                    # Test the connection by requesting metadata
                    client.topics()
                    return client
                elif client_type == "producer":
                    client = KafkaProducer(**kafka_config)
                    return client
                elif client_type == "admin":
                    # Filter out parameters not supported by KafkaAdminClient
                    admin_config = kafka_config.copy()
                    if 'session_timeout_ms' in admin_config:
                        admin_config.pop('session_timeout_ms')
                    client = KafkaAdminClient(**admin_config)
                    return client
                else:
                    raise KafkaConnectionError(f"Unknown client type: {client_type}")
            except NoBrokersAvailable as e:
                last_error = e
                logger.warning(f"Connection attempt {retries + 1} failed for {cluster_name}: {e}")
                retries += 1
                # Exponential backoff
                time.sleep(2 ** retries)
            except KafkaError as e:
                last_error = e
                logger.warning(f"Kafka error during connection for {cluster_name}: {e}")
                retries += 1
                time.sleep(2 ** retries)
            except Exception as e:
                last_error = e
                logger.exception(f"Unexpected error during connection for {cluster_name}")
                retries += 1
                time.sleep(2 ** retries)
        
        # Failed after retries
        error_msg = f"Failed to connect to cluster {cluster_name} after {max_retries} attempts"
        if last_error:
            error_msg = f"{error_msg}: {last_error}"
        
        logger.error(error_msg)
        raise KafkaConnectionError(error_msg)
    
    def connect(self, cluster_name: str, client_type: str = "consumer") -> Any:
        """
        Establish a connection to a Kafka cluster.
        
        Args:
            cluster_name: Name of the cluster
            client_type: Type of client ("consumer", "producer", "admin")
            
        Returns:
            Kafka client instance
            
        Raises:
            KafkaConnectionError: If connection fails
        """
        connection_key = f"{cluster_name}_{client_type}"
        
        # Check if connection already exists
        if connection_key in self.cluster_connections:
            logger.debug(f"Reusing existing {client_type} connection for {cluster_name}")
            return self.cluster_connections[connection_key]["client"]
        
        logger.info(f"Establishing new {client_type} connection for cluster {cluster_name}")
        
        try:
            # Get cluster configuration
            cluster_props = self._get_cluster_config(cluster_name)
            
            # Create Kafka configuration
            kafka_config = self._create_kafka_config(cluster_props)
            
            # Establish connection
            client = self._try_connect(cluster_name, kafka_config, client_type)
            
            # Store connection
            self.cluster_connections[connection_key] = {
                "client": client,
                "type": client_type,
                "config": kafka_config,
                "created_at": time.time()
            }
            
            self.active_connections.add(connection_key)
            
            logger.info(f"Successfully connected to cluster {cluster_name} with {client_type} client")
            return client
            
        except Exception as e:
            logger.exception(f"Error connecting to cluster {cluster_name}")
            raise KafkaConnectionError(f"Failed to connect to cluster {cluster_name}: {e}")
    
    def get_consumer(self, cluster_name: str, topics: List[str] = None, **kwargs) -> KafkaConsumer:
        """
        Get a Kafka consumer for a specific cluster.
        
        Args:
            cluster_name: Name of the cluster
            topics: List of topics to subscribe to
            **kwargs: Additional parameters for the consumer
            
        Returns:
            KafkaConsumer instance
            
        Raises:
            KafkaConnectionError: If connection fails
        """
        # Get cluster configuration
        cluster_props = self._get_cluster_config(cluster_name)
        
        # Create Kafka configuration
        kafka_config = self._create_kafka_config(cluster_props)
        
        # Add additional parameters
        kafka_config.update(kwargs)
        
        # Create a unique connection key based on config hash
        config_str = str(sorted(kafka_config.items()))
        connection_key = f"{cluster_name}_consumer_{hash(config_str)}"
        
        # Check if connection already exists
        if connection_key in self.cluster_connections:
            consumer = self.cluster_connections[connection_key]["client"]
            # Subscribe to topics if provided
            if topics:
                consumer.subscribe(topics)
            return consumer
        
        # Create new consumer
        logger.info(f"Creating new consumer for cluster {cluster_name}")
        try:
            if topics:
                consumer = KafkaConsumer(*topics, **kafka_config)
            else:
                consumer = KafkaConsumer(**kafka_config)
                
            # Store connection
            self.cluster_connections[connection_key] = {
                "client": consumer,
                "type": "consumer",
                "config": kafka_config,
                "created_at": time.time()
            }
            
            self.active_connections.add(connection_key)
            
            return consumer
        except Exception as e:
            logger.exception(f"Error creating consumer for cluster {cluster_name}")
            raise KafkaConnectionError(f"Failed to create consumer for cluster {cluster_name}: {e}")
    
    def get_admin_client(self, cluster_name: str, **kwargs) -> KafkaAdminClient:
        """
        Get a Kafka admin client for a specific cluster.
        
        Args:
            cluster_name: Name of the cluster
            **kwargs: Additional parameters for the admin client
            
        Returns:
            KafkaAdminClient instance
            
        Raises:
            KafkaConnectionError: If connection fails
        """
        connection_key = f"{cluster_name}_admin"
        
        # Check if connection already exists
        if connection_key in self.cluster_connections:
            return self.cluster_connections[connection_key]["client"]
        
        # Get cluster configuration
        cluster_props = self._get_cluster_config(cluster_name)
        
        # Create Kafka configuration
        kafka_config = self._create_kafka_config(cluster_props)
        
        # Add additional parameters
        kafka_config.update(kwargs)
        
        # Create admin client
        logger.info(f"Creating admin client for cluster {cluster_name}")
        
        # Filter out parameters not supported by KafkaAdminClient
        admin_config = kafka_config.copy()
        if 'session_timeout_ms' in admin_config:
            admin_config.pop('session_timeout_ms')
        
        try:
            admin_client = KafkaAdminClient(**admin_config)
            
            # Store connection
            self.cluster_connections[connection_key] = {
                "client": admin_client,
                "type": "admin",
                "config": kafka_config,
                "created_at": time.time()
            }
            
            self.active_connections.add(connection_key)
            
            return admin_client
        except Exception as e:
            logger.exception(f"Error creating admin client for cluster {cluster_name}")
            raise KafkaConnectionError(f"Failed to create admin client for cluster {cluster_name}: {e}")
    
    def list_topics(self, cluster_name: str) -> List[str]:
        """
        List topics available in a Kafka cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of topic names
            
        Raises:
            KafkaConnectionError: If connection fails
        """
        try:
            consumer = self.connect(cluster_name, client_type="consumer")
            topics = consumer.topics()
            return sorted(list(topics))
        except Exception as e:
            logger.exception(f"Error listing topics for cluster {cluster_name}")
            raise KafkaConnectionError(f"Failed to list topics for cluster {cluster_name}: {e}")
    
    def close_connection(self, cluster_name: str, client_type: str = None):
        """
        Close a specific connection or all connections for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            client_type: Type of client to close, or None for all
        """
        # Find connection keys to close
        keys_to_close = []
        for key in list(self.cluster_connections.keys()):
            if key.startswith(f"{cluster_name}_"):
                if client_type is None or key == f"{cluster_name}_{client_type}":
                    keys_to_close.append(key)
        
        # Close connections
        for key in keys_to_close:
            try:
                client = self.cluster_connections[key]["client"]
                client.close()
                self.cluster_connections.pop(key)
                if key in self.active_connections:
                    self.active_connections.remove(key)
                logger.info(f"Closed connection {key}")
            except Exception as e:
                logger.warning(f"Error closing connection {key}: {e}")
    
    def close_all_connections(self):
        """Close all Kafka connections."""
        for key in list(self.cluster_connections.keys()):
            try:
                client = self.cluster_connections[key]["client"]
                client.close()
                logger.info(f"Closed connection {key}")
            except Exception as e:
                logger.warning(f"Error closing connection {key}: {e}")
        
        self.cluster_connections.clear()
        self.active_connections.clear()
    
    def __del__(self):
        """Cleanup when the connector is garbage collected."""
        try:
            self.close_all_connections()
            self.executor.shutdown(wait=False)
        except Exception:
            # Ignore errors during cleanup
            pass
