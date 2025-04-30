"""
Configuration commands for the KDIFF CLI.

This module provides CLI commands for managing KDIFF configuration.
"""

import os
import logging
from typing import Optional

from kdiff.core.config import KDiffConfig, ConfigError
from kdiff.core.cluster_manager import ClusterManager
from kdiff.utils.file_utils import expand_path

logger = logging.getLogger(__name__)


def handle_config_cluster_command(args):
    """
    Handle the 'config cluster' command.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load configuration
    config_dir = args.config_dir
    kdiff_config = KDiffConfig(config_dir=config_dir)
    
    # Create cluster manager
    clusters_dir = os.path.join(kdiff_config.config_dir, "clusters")
    clusters_config_path = os.path.join(kdiff_config.config_dir, "clusters.yaml")
    manager = ClusterManager(clusters_dir, clusters_config_path)
    
    try:
        # Handle different operations
        if args.list:
            return list_clusters(manager)
        elif args.add:
            return add_cluster(manager, args.add, args.name, args.force)
        elif args.remove:
            return remove_cluster(manager, args.remove)
        else:
            print("Error: No operation specified. Use --add, --list, or --remove.")
            return 1
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        print(f"Error: {e}")
        return 1
    except Exception as e:
        logger.exception("An error occurred")
        print(f"Error: {e}")
        return 1


def list_clusters(manager: ClusterManager) -> int:
    """
    List all configured clusters.
    
    Args:
        manager: The cluster manager
        
    Returns:
        Exit code (0 for success)
    """
    clusters = manager.get_cluster_details()
    
    if not clusters:
        print("No clusters configured.")
        return 0
        
    print("Configured clusters:")
    for cluster in clusters:
        alias_str = " (alias)" if cluster["alias"] else ""
        print(f"  - {cluster['name']}{alias_str}")
        print(f"    Bootstrap servers: {cluster['bootstrap_servers']}")
        print(f"    Security protocol: {cluster['security_protocol']}")
        print("")
        
    return 0


def add_cluster(manager: ClusterManager, properties_path: str, name: Optional[str] = None, force: bool = False) -> int:
    """
    Add a new cluster configuration.
    
    Args:
        manager: The cluster manager
        properties_path: Path to the properties file
        name: Name for the cluster (derived from file if not provided)
        force: Whether to force overwrite
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Expand path
    properties_path = expand_path(properties_path)
    
    # Validate file
    if not os.path.isfile(properties_path):
        print(f"Error: File not found: {properties_path}")
        return 1
        
    # Derive name from file if not provided
    if not name:
        name = os.path.basename(properties_path)
        if name.endswith(".properties"):
            name = name[:-11]  # Remove .properties suffix
            
    print(f"Adding cluster configuration '{name}' from {properties_path}...")
    
    try:
        # Load and validate properties
        success, message = manager.add_cluster_config(properties_path, name, force)
        
        if success:
            print(f"Success: {message}")
            return 0
        else:
            print(f"Error: {message}")
            return 1
    except ConfigError as e:
        print(f"Error: {e}")
        return 1


def remove_cluster(manager: ClusterManager, name: str) -> int:
    """
    Remove a cluster configuration.
    
    Args:
        manager: The cluster manager
        name: Name of the cluster to remove
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    print(f"Removing cluster configuration '{name}'...")
    
    try:
        success, message = manager.remove_cluster_config(name)
        
        if success:
            print(f"Success: {message}")
            return 0
        else:
            print(f"Error: {message}")
            return 1
    except ConfigError as e:
        print(f"Error: {e}")
        return 1
