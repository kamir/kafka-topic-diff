"""
Configuration module for KDIFF.

This module handles loading and managing configuration from various sources
including YAML files, environment variables, and defaults.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path

from kdiff.utils.file_utils import expand_path, ensure_dir_exists, find_file
from kdiff.core.constants import (
    DEFAULT_KTOOLS_DIR,
    DEFAULT_CLUSTERS_DIR,
    DEFAULT_KDIFF_CONFIG,
    DEFAULT_CLUSTERS_CONFIG,
    DEFAULT_CONFIG,
    DEFAULT_CLUSTERS_CONFIG_CONTENT,
    ENV_PREFIX,
    ENV_CONFIG_DIR,
    ENV_CLUSTER_A,
    ENV_CLUSTER_B,
)

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Exception raised for configuration errors."""
    pass


class KDiffConfig:
    """
    Configuration manager for KDIFF.
    
    This class handles loading and managing configuration from YAML files,
    environment variables, and defaults.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_dir: Path to the configuration directory (defaults to env var or ~/ktools)
        """
        # Initialize with defaults
        self.config = DEFAULT_CONFIG.copy()
        self.clusters_config = DEFAULT_CLUSTERS_CONFIG_CONTENT.copy()
        self.cluster_configs: Dict[str, Dict[str, Any]] = {}
        
        # Determine config directory
        self.config_dir = config_dir or os.environ.get(ENV_CONFIG_DIR) or DEFAULT_KTOOLS_DIR
        self.config_dir = expand_path(self.config_dir)
        self.clusters_dir = os.path.join(self.config_dir, "clusters")
        
        # Load configurations
        try:
            self._load_kdiff_config()
            self._load_clusters_config()
            self._apply_env_overrides()
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise ConfigError(f"Failed to load configuration: {e}")
    
    def _load_yaml_file(self, filepath: str, default: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load and parse a YAML file.
        
        Args:
            filepath: Path to the YAML file
            default: Default configuration to use if file doesn't exist
            
        Returns:
            Parsed YAML content as dictionary
        """
        expanded_path = expand_path(filepath)
        if not os.path.exists(expanded_path):
            logger.warning(f"Configuration file not found: {expanded_path}")
            return default.copy()
            
        try:
            with open(expanded_path, 'r') as file:
                return yaml.safe_load(file) or default.copy()
        except Exception as e:
            logger.error(f"Error parsing YAML file {expanded_path}: {e}")
            return default.copy()
    
    def _load_kdiff_config(self):
        """Load the main KDIFF configuration file."""
        config_path = os.path.join(self.config_dir, "kdiff.yaml")
        loaded_config = self._load_yaml_file(config_path, DEFAULT_CONFIG)
        
        # Merge with defaults, ensuring all required keys exist
        if "default" in loaded_config:
            # Handle nested dictionaries to ensure all default keys exist
            for key, value in loaded_config["default"].items():
                if isinstance(value, dict) and key in self.config["default"] and isinstance(self.config["default"][key], dict):
                    # Merge nested dictionaries instead of replacing
                    self.config["default"][key].update(value)
                else:
                    # For non-dictionary values, simple replacement
                    self.config["default"][key] = value
                    
        if "service" in loaded_config:
            # Handle nested dictionaries for service config too
            for key, value in loaded_config["service"].items():
                if isinstance(value, dict) and key in self.config["service"] and isinstance(self.config["service"][key], dict):
                    self.config["service"][key].update(value)
                else:
                    self.config["service"][key] = value
    
    def _load_clusters_config(self):
        """Load the clusters configuration file."""
        clusters_config_path = os.path.join(self.config_dir, "clusters.yaml")
        self.clusters_config = self._load_yaml_file(
            clusters_config_path, DEFAULT_CLUSTERS_CONFIG_CONTENT
        )
        
        # Create clusters directory if it doesn't exist
        ensure_dir_exists(self.clusters_dir)
        
        # Load all available cluster configurations
        self._load_available_cluster_configs()
    
    def _load_available_cluster_configs(self):
        """Load all available cluster configurations from the clusters directory."""
        if not os.path.exists(self.clusters_dir):
            logger.warning(f"Clusters directory not found: {self.clusters_dir}")
            return
            
        for filename in os.listdir(self.clusters_dir):
            if filename.startswith("cluster-client-cfg-"):
                cluster_name = filename.replace("cluster-client-cfg-", "")
                filepath = os.path.join(self.clusters_dir, filename)
                self.cluster_configs[cluster_name] = self._load_cluster_properties(filepath)
    
    def _load_cluster_properties(self, filepath: str) -> Dict[str, str]:
        """
        Load Kafka client properties file.
        
        Args:
            filepath: Path to the properties file
            
        Returns:
            Dictionary of properties
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
        
        return properties
    
    def _apply_env_overrides(self):
        """Apply environment variable overrides to the configuration."""
        # Apply environment variables with prefix
        for key, value in os.environ.items():
            if key.startswith(ENV_PREFIX):
                config_key = key[len(ENV_PREFIX):].lower()
                
                # Special case for nested keys using double underscore
                if '__' in config_key:
                    section, option = config_key.split('__', 1)
                    if section in self.config:
                        try:
                            # Try to convert value to appropriate type (bool, int)
                            if value.lower() in ('true', 'yes', '1'):
                                self.config[section][option] = True
                            elif value.lower() in ('false', 'no', '0'):
                                self.config[section][option] = False
                            elif value.isdigit():
                                self.config[section][option] = int(value)
                            else:
                                self.config[section][option] = value
                        except Exception as e:
                            logger.warning(f"Failed to override config with {key}={value}: {e}")
    
    def get_cluster_config(self, cluster_name: str) -> Dict[str, str]:
        """
        Get the configuration for a specific cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Dictionary of cluster configuration properties
            
        Raises:
            ConfigError: If the cluster configuration is not found
        """
        # Check if this is an alias in clusters.yaml
        cluster_alias = None
        if 'clusters' in self.clusters_config and cluster_name in self.clusters_config['clusters']:
            cluster_alias = self.clusters_config['clusters'][cluster_name].get('config_file')
            if cluster_alias:
                cluster_alias = cluster_alias.replace("cluster-client-cfg-", "")
                
        # Find the actual cluster config
        target_cluster = cluster_alias or cluster_name
        
        if target_cluster in self.cluster_configs:
            return self.cluster_configs[target_cluster]
        
        # If not found, check environment variables
        if cluster_name == "cluster-a" and ENV_CLUSTER_A in os.environ:
            env_cluster = os.environ[ENV_CLUSTER_A]
            if env_cluster in self.cluster_configs:
                return self.cluster_configs[env_cluster]
                
        if cluster_name == "cluster-b" and ENV_CLUSTER_B in os.environ:
            env_cluster = os.environ[ENV_CLUSTER_B]
            if env_cluster in self.cluster_configs:
                return self.cluster_configs[env_cluster]
        
        raise ConfigError(f"Cluster configuration not found for: {cluster_name}")
    
    def get_available_clusters(self) -> List[str]:
        """
        Get a list of available cluster names.
        
        Returns:
            List of cluster names
        """
        clusters = list(self.cluster_configs.keys())
        
        # Add aliases from clusters.yaml
        if 'clusters' in self.clusters_config:
            for alias in self.clusters_config['clusters'].keys():
                if alias not in clusters:
                    clusters.append(alias)
        
        return sorted(clusters)
    
    def get_config_value(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            section: Configuration section (e.g., "default", "service")
            key: Configuration key
            default: Default value to return if not found
            
        Returns:
            Configuration value or default
        """
        if section in self.config and key in self.config[section]:
            return self.config[section][key]
        return default
    
    def get_comparison_options(self) -> Dict[str, bool]:
        """
        Get the comparison options.
        
        Returns:
            Dictionary of comparison options
        """
        return self.config["default"]["comparison"].copy()
    
    def get_default_config(self) -> Dict[str, Any]:
        """
        Get the default configuration section.
        
        Returns:
            Dictionary of default configuration
        """
        return self.config["default"].copy()
    
    def get_service_config(self) -> Dict[str, Any]:
        """
        Get the service configuration section.
        
        Returns:
            Dictionary of service configuration
        """
        return self.config["service"].copy()
    
    def create_default_configs(self) -> None:
        """
        Create default configuration files if they don't exist.
        
        This method creates the default configuration structure in the
        specified configuration directory.
        """
        # Create config directory
        config_dir = ensure_dir_exists(self.config_dir)
        
        # Create clusters directory
        clusters_dir = ensure_dir_exists(os.path.join(config_dir, "clusters"))
        
        # Create kdiff.yaml if it doesn't exist
        kdiff_config_path = os.path.join(config_dir, "kdiff.yaml")
        if not os.path.exists(kdiff_config_path):
            with open(kdiff_config_path, 'w') as file:
                yaml.dump(DEFAULT_CONFIG, file, default_flow_style=False)
            
        # Create clusters.yaml if it doesn't exist
        clusters_config_path = os.path.join(config_dir, "clusters.yaml")
        if not os.path.exists(clusters_config_path):
            with open(clusters_config_path, 'w') as file:
                yaml.dump(DEFAULT_CLUSTERS_CONFIG_CONTENT, file, default_flow_style=False)
