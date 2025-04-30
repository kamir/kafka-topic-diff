"""
Cluster configuration management for KDIFF.

This module provides functionality to manage Kafka cluster configurations.
"""

import os
import shutil
import logging
import yaml
from typing import Dict, Any, Optional, List, Tuple

from kdiff.utils.file_utils import expand_path, ensure_dir_exists
from kdiff.core.constants import DEFAULT_CLUSTERS_DIR
from kdiff.core.config import ConfigError

logger = logging.getLogger(__name__)


class ClusterConfigValidator:
    """Validator for Kafka cluster configurations."""
    
    # Required properties that must exist in a valid client.properties file
    REQUIRED_PROPERTIES = ["bootstrap.servers"]
    
    # Recommended but optional properties
    RECOMMENDED_PROPERTIES = ["security.protocol", "client.id"]
    
    @classmethod
    def validate(cls, properties: Dict[str, str]) -> Tuple[bool, Optional[str]]:
        """
        Validate a cluster configuration.
        
        Args:
            properties: Dictionary of properties from client.properties
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for required properties
        for prop in cls.REQUIRED_PROPERTIES:
            if prop not in properties:
                return False, f"Missing required property: {prop}"
        
        # Check for recommended properties
        for prop in cls.RECOMMENDED_PROPERTIES:
            if prop not in properties:
                logger.warning(f"Recommended property not found: {prop}")
        
        # Additional validation can be added here as needed
        
        return True, None


class ClusterManager:
    """Manager for Kafka cluster configurations."""
    
    def __init__(self, clusters_dir: str, clusters_config_path: str):
        """
        Initialize the cluster manager.
        
        Args:
            clusters_dir: Path to clusters directory
            clusters_config_path: Path to clusters YAML configuration file
        """
        self.clusters_dir = expand_path(clusters_dir)
        self.clusters_config_path = expand_path(clusters_config_path)
        
        # Ensure directories exist
        ensure_dir_exists(self.clusters_dir)
    
    def load_clusters_config(self) -> Dict[str, Any]:
        """
        Load the clusters configuration file.
        
        Returns:
            The clusters configuration dictionary
        """
        if not os.path.exists(self.clusters_config_path):
            return {"clusters": {}}
            
        try:
            with open(self.clusters_config_path, 'r') as file:
                config = yaml.safe_load(file)
                if not config or not isinstance(config, dict):
                    return {"clusters": {}}
                if "clusters" not in config:
                    config["clusters"] = {}
                return config
        except Exception as e:
            logger.error(f"Error loading clusters config: {e}")
            return {"clusters": {}}
    
    def save_clusters_config(self, config: Dict[str, Any]) -> bool:
        """
        Save the clusters configuration file.
        
        Args:
            config: The clusters configuration dictionary
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            with open(self.clusters_config_path, 'w') as file:
                yaml.dump(config, file, default_flow_style=False)
            return True
        except Exception as e:
            logger.error(f"Error saving clusters config: {e}")
            return False
    
    def load_properties_file(self, filepath: str) -> Dict[str, str]:
        """
        Load a properties file into a dictionary.
        
        Args:
            filepath: Path to the properties file
            
        Returns:
            Dictionary of properties
            
        Raises:
            ConfigError: If the file cannot be loaded
        """
        properties = {}
        try:
            with open(filepath, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' in line:
                            key, value = line.split('=', 1)
                            properties[key.strip()] = value.strip()
        except Exception as e:
            logger.error(f"Error loading properties file {filepath}: {e}")
            raise ConfigError(f"Failed to load properties file: {e}")
        
        return properties
    
    def is_cluster_configured(self, properties: Dict[str, str]) -> Tuple[bool, Optional[str]]:
        """
        Check if a cluster is already configured based on its properties.
        
        Args:
            properties: Dictionary of properties from client.properties
            
        Returns:
            Tuple of (is_configured, cluster_name)
        """
        if "bootstrap.servers" not in properties:
            return False, None
            
        bootstrap_servers = properties["bootstrap.servers"]
        
        # Check each cluster config file in the clusters directory
        if os.path.exists(self.clusters_dir):
            for filename in os.listdir(self.clusters_dir):
                if filename.startswith("cluster-client-cfg-"):
                    filepath = os.path.join(self.clusters_dir, filename)
                    try:
                        existing_props = self.load_properties_file(filepath)
                        if "bootstrap.servers" in existing_props and existing_props["bootstrap.servers"] == bootstrap_servers:
                            cluster_name = filename.replace("cluster-client-cfg-", "")
                            return True, cluster_name
                    except Exception:
                        continue
        
        return False, None
    
    def add_cluster_config(self, properties_path: str, cluster_name: str, force: bool = False) -> Tuple[bool, str]:
        """
        Add a new cluster configuration from a properties file.
        
        Args:
            properties_path: Path to the client.properties file
            cluster_name: Name to assign to the cluster
            force: Whether to overwrite existing configuration
            
        Returns:
            Tuple of (success, message)
            
        Raises:
            ConfigError: If the configuration is invalid or cannot be added
        """
        # Load and validate properties
        try:
            properties = self.load_properties_file(properties_path)
        except Exception as e:
            raise ConfigError(f"Failed to load properties file: {e}")
            
        is_valid, error = ClusterConfigValidator.validate(properties)
        if not is_valid:
            raise ConfigError(f"Invalid cluster configuration: {error}")
            
        # Check if already configured
        is_configured, existing_name = self.is_cluster_configured(properties)
        if is_configured and not force:
            return False, f"Cluster is already configured with name: {existing_name}"
            
        # Copy properties file to clusters directory
        target_filename = f"cluster-client-cfg-{cluster_name}"
        target_path = os.path.join(self.clusters_dir, target_filename)
        
        try:
            shutil.copy2(properties_path, target_path)
        except Exception as e:
            raise ConfigError(f"Failed to copy properties file: {e}")
            
        # Update clusters.yaml if needed
        config = self.load_clusters_config()
        config["clusters"][cluster_name] = {
            "config_file": target_filename
        }
        
        if not self.save_clusters_config(config):
            # Try to clean up the copied file if config save fails
            if os.path.exists(target_path):
                try:
                    os.remove(target_path)
                except Exception:
                    pass
            raise ConfigError("Failed to update clusters configuration")
            
        return True, f"Cluster '{cluster_name}' added successfully"
    
    def remove_cluster_config(self, cluster_name: str) -> Tuple[bool, str]:
        """
        Remove a cluster configuration.
        
        Args:
            cluster_name: Name of the cluster to remove
            
        Returns:
            Tuple of (success, message)
        """
        # Load clusters config
        config = self.load_clusters_config()
        
        # Check if cluster exists in clusters.yaml
        if cluster_name not in config["clusters"]:
            return False, f"Cluster '{cluster_name}' not found in configuration"
            
        # Get the config file name
        config_file = config["clusters"][cluster_name].get("config_file")
        if not config_file:
            config_file = f"cluster-client-cfg-{cluster_name}"
            
        # Remove from clusters.yaml
        del config["clusters"][cluster_name]
        if not self.save_clusters_config(config):
            return False, "Failed to update clusters configuration"
            
        # Remove the properties file
        file_path = os.path.join(self.clusters_dir, config_file)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                return False, f"Failed to remove cluster file: {e}"
                
        return True, f"Cluster '{cluster_name}' removed successfully"
    
    def get_cluster_config(self, cluster_name: str) -> Dict[str, str]:
        """
        Get the configuration properties for a specific cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Dictionary of properties for the cluster
            
        Raises:
            ConfigError: If the cluster configuration is not found
        """
        config = self.load_clusters_config()
        
        # Check if cluster exists in config
        if cluster_name not in config["clusters"]:
            raise ConfigError(f"Cluster configuration not found for: {cluster_name}")
            
        # Get the config file name
        config_file = config["clusters"][cluster_name].get("config_file")
        if not config_file:
            config_file = f"cluster-client-cfg-{cluster_name}"
            
        # Load the properties file
        file_path = os.path.join(self.clusters_dir, config_file)
        if not os.path.exists(file_path):
            raise ConfigError(f"Cluster properties file not found: {file_path}")
            
        return self.load_properties_file(file_path)
    
    def get_cluster_details(self) -> List[Dict[str, Any]]:
        """
        Get details of all configured clusters.
        
        Returns:
            List of dictionaries with cluster details
        """
        result = []
        config = self.load_clusters_config()
        
        # Find all cluster config files
        if os.path.exists(self.clusters_dir):
            # First, get properties from direct files
            for filename in os.listdir(self.clusters_dir):
                if filename.startswith("cluster-client-cfg-"):
                    cluster_name = filename.replace("cluster-client-cfg-", "")
                    filepath = os.path.join(self.clusters_dir, filename)
                    
                    try:
                        properties = self.load_properties_file(filepath)
                        bootstrap_servers = properties.get("bootstrap.servers", "unknown")
                        security_protocol = properties.get("security.protocol", "PLAINTEXT")
                        
                        result.append({
                            "name": cluster_name,
                            "bootstrap_servers": bootstrap_servers,
                            "security_protocol": security_protocol,
                            "alias": False,
                            "file": filename
                        })
                    except Exception:
                        # Skip files that can't be loaded
                        continue
            
            # Then, add any aliases from clusters.yaml
            for alias, details in config.get("clusters", {}).items():
                if "config_file" in details:
                    config_file = details["config_file"]
                    for cluster in result:
                        if cluster["file"] == config_file or cluster["name"] == config_file.replace("cluster-client-cfg-", ""):
                            result.append({
                                "name": alias,
                                "bootstrap_servers": cluster["bootstrap_servers"],
                                "security_protocol": cluster["security_protocol"],
                                "alias": True,
                                "file": config_file
                            })
                            break
        
        return result
