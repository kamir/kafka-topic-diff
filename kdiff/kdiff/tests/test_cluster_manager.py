"""
Unit tests for the KDIFF cluster manager.
"""

import os
import tempfile
import unittest
import yaml
import shutil
from pathlib import Path

from kdiff.core.cluster_manager import ClusterManager, ClusterConfigValidator, ConfigError


class TestClusterConfigValidator(unittest.TestCase):
    """Tests for the ClusterConfigValidator class."""
    
    def test_valid_config(self):
        """Test that a valid configuration passes validation."""
        props = {
            "bootstrap.servers": "test-broker:9092",
            "security.protocol": "SASL_SSL",
            "client.id": "test-client"
        }
        
        is_valid, error = ClusterConfigValidator.validate(props)
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_missing_required_property(self):
        """Test that missing required properties fail validation."""
        props = {
            "security.protocol": "SASL_SSL",
            "client.id": "test-client"
        }
        
        is_valid, error = ClusterConfigValidator.validate(props)
        self.assertFalse(is_valid)
        self.assertIn("bootstrap.servers", error)


class TestClusterManager(unittest.TestCase):
    """Tests for the ClusterManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test configs
        self.test_dir = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.test_dir.name)
        self.clusters_dir = self.config_dir / "clusters"
        self.clusters_dir.mkdir(exist_ok=True)
        self.clusters_config_path = self.config_dir / "clusters.yaml"
        
        # Create test properties file
        self.test_props_dir = tempfile.TemporaryDirectory()
        self.test_props_path = Path(self.test_props_dir.name) / "test-cluster.properties"
        with open(self.test_props_path, 'w') as f:
            f.write("bootstrap.servers=test-broker:9092\n")
            f.write("security.protocol=SASL_SSL\n")
            f.write("client.id=test-client\n")
        
        # Create test manager
        self.manager = ClusterManager(str(self.clusters_dir), str(self.clusters_config_path))
    
    def tearDown(self):
        """Clean up test environment."""
        self.test_dir.cleanup()
        self.test_props_dir.cleanup()
    
    def test_load_save_clusters_config(self):
        """Test loading and saving clusters configuration."""
        # Save a test config
        test_config = {
            "clusters": {
                "test-cluster": {
                    "config_file": "cluster-client-cfg-test-cluster"
                }
            }
        }
        
        self.assertTrue(self.manager.save_clusters_config(test_config))
        
        # Load the config
        loaded_config = self.manager.load_clusters_config()
        self.assertEqual(loaded_config, test_config)
    
    def test_load_properties_file(self):
        """Test loading properties file."""
        properties = self.manager.load_properties_file(str(self.test_props_path))
        
        self.assertEqual(properties["bootstrap.servers"], "test-broker:9092")
        self.assertEqual(properties["security.protocol"], "SASL_SSL")
        self.assertEqual(properties["client.id"], "test-client")
    
    def test_add_cluster_config(self):
        """Test adding a cluster configuration."""
        success, message = self.manager.add_cluster_config(
            str(self.test_props_path), "test-cluster"
        )
        
        self.assertTrue(success)
        self.assertIn("test-cluster", message)
        
        # Check that the file was copied
        target_path = self.clusters_dir / "cluster-client-cfg-test-cluster"
        self.assertTrue(target_path.exists())
        
        # Check that clusters.yaml was updated
        config = self.manager.load_clusters_config()
        self.assertIn("test-cluster", config["clusters"])
        self.assertEqual(
            config["clusters"]["test-cluster"]["config_file"],
            "cluster-client-cfg-test-cluster"
        )
        
        # Check is_cluster_configured
        is_configured, name = self.manager.is_cluster_configured({
            "bootstrap.servers": "test-broker:9092"
        })
        self.assertTrue(is_configured)
        self.assertEqual(name, "test-cluster")
    
    def test_add_existing_cluster(self):
        """Test adding a cluster that already exists."""
        # First add the cluster
        self.manager.add_cluster_config(str(self.test_props_path), "test-cluster")
        
        # Try to add it again
        success, message = self.manager.add_cluster_config(
            str(self.test_props_path), "another-name"
        )
        
        self.assertFalse(success)
        self.assertIn("already configured", message)
        self.assertIn("test-cluster", message)
    
    def test_force_add_existing_cluster(self):
        """Test force adding a cluster that already exists."""
        # First add the cluster
        self.manager.add_cluster_config(str(self.test_props_path), "test-cluster")
        
        # Try to add it again with force
        success, message = self.manager.add_cluster_config(
            str(self.test_props_path), "another-name", force=True
        )
        
        self.assertTrue(success)
        self.assertIn("another-name", message)
        
        # Check clusters.yaml
        config = self.manager.load_clusters_config()
        self.assertIn("another-name", config["clusters"])
    
    def test_remove_cluster_config(self):
        """Test removing a cluster configuration."""
        # First add the cluster
        self.manager.add_cluster_config(str(self.test_props_path), "test-cluster")
        
        # Then remove it
        success, message = self.manager.remove_cluster_config("test-cluster")
        
        self.assertTrue(success)
        self.assertIn("removed successfully", message)
        
        # Check that the file was removed
        target_path = self.clusters_dir / "cluster-client-cfg-test-cluster"
        self.assertFalse(target_path.exists())
        
        # Check that clusters.yaml was updated
        config = self.manager.load_clusters_config()
        self.assertNotIn("test-cluster", config["clusters"])
    
    def test_remove_nonexistent_cluster(self):
        """Test removing a cluster that doesn't exist."""
        success, message = self.manager.remove_cluster_config("nonexistent-cluster")
        
        self.assertFalse(success)
        self.assertIn("not found", message)
    
    def test_get_cluster_details(self):
        """Test getting cluster details."""
        # Create a fresh manager for this test to avoid interference
        test_dir = tempfile.TemporaryDirectory()
        clusters_dir = Path(test_dir.name) / "clusters"
        clusters_dir.mkdir(exist_ok=True)
        clusters_config_path = Path(test_dir.name) / "clusters.yaml"
        manager = ClusterManager(str(clusters_dir), str(clusters_config_path))
        
        try:
            # Add a cluster
            manager.add_cluster_config(str(self.test_props_path), "test-cluster")
            
            # Add an alias
            config = manager.load_clusters_config()
            config["clusters"]["test-alias"] = {
                "config_file": "cluster-client-cfg-test-cluster"
            }
            manager.save_clusters_config(config)
            
            # Get details
            details = manager.get_cluster_details()
            
            # Find the real cluster and alias
            real_cluster = None
            alias_cluster = None
            for detail in details:
                if detail["name"] == "test-cluster" and not detail["alias"]:
                    real_cluster = detail
                elif detail["name"] == "test-alias" and detail["alias"]:
                    alias_cluster = detail
            
            # Verify we found both clusters
            self.assertIsNotNone(real_cluster)
            self.assertIsNotNone(alias_cluster)
            self.assertEqual(real_cluster["bootstrap_servers"], "test-broker:9092")
            self.assertEqual(alias_cluster["bootstrap_servers"], "test-broker:9092")
        finally:
            test_dir.cleanup()


if __name__ == '__main__':
    unittest.main()
