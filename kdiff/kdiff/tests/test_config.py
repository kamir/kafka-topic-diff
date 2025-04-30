"""
Unit tests for the KDIFF configuration module.
"""

import os
import tempfile
import unittest
import yaml
from pathlib import Path

from kdiff.core.config import KDiffConfig, ConfigError
from kdiff.core.constants import DEFAULT_CONFIG, DEFAULT_CLUSTERS_CONFIG_CONTENT, ENV_PREFIX


class TestKDiffConfig(unittest.TestCase):
    """Tests for the KDiffConfig class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test configs
        self.test_dir = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.test_dir.name)
        self.clusters_dir = self.config_dir / "clusters"
        self.clusters_dir.mkdir(exist_ok=True)
        
        # Create test kdiff.yaml
        self.kdiff_config = self.config_dir / "kdiff.yaml"
        with open(self.kdiff_config, 'w') as f:
            yaml.dump({
                "default": {
                    "checksum_algorithm": "MD5",
                    "max_messages": 500,
                    "comparison": {
                        "content": True,
                        "offsets": True
                    }
                }
            }, f)
        
        # Create test clusters.yaml
        self.clusters_config = self.config_dir / "clusters.yaml"
        with open(self.clusters_config, 'w') as f:
            yaml.dump({
                "clusters": {
                    "test-alias": {
                        "config_file": "cluster-client-cfg-test-cluster"
                    }
                }
            }, f)
        
        # Create test cluster config
        self.cluster_config = self.clusters_dir / "cluster-client-cfg-test-cluster"
        with open(self.cluster_config, 'w') as f:
            f.write("bootstrap.servers=test-broker:9092\n")
            f.write("security.protocol=SASL_SSL\n")
        
        # Clear environment variables that might affect tests
        for key in list(os.environ.keys()):
            if key.startswith(ENV_PREFIX):
                del os.environ[key]
    
    def tearDown(self):
        """Clean up test environment."""
        self.test_dir.cleanup()
    
    def test_default_config_values(self):
        """Test that default configuration values are used correctly."""
        config = KDiffConfig(config_dir=self.config_dir)
        
        # Check that values from test config are loaded
        self.assertEqual(config.get_config_value("default", "checksum_algorithm"), "MD5")
        self.assertEqual(config.get_config_value("default", "max_messages"), 500)
        
        # Check that default values are used for missing keys
        self.assertEqual(config.get_config_value("default", "strategy"), "stream")
        self.assertEqual(config.get_config_value("default", "output_format"), "text")
        
        # Check comparison options
        comparison = config.get_comparison_options()
        self.assertTrue(comparison["content"])
        self.assertTrue(comparison["offsets"])
        self.assertFalse(comparison["timestamps"])
        self.assertFalse(comparison["schema_ids"])
    
    def test_environment_overrides(self):
        """Test that environment variables override configuration values."""
        # Set environment variables
        os.environ[f"{ENV_PREFIX}DEFAULT__MAX_MESSAGES"] = "1000"
        os.environ[f"{ENV_PREFIX}DEFAULT__STRATEGY"] = "table"
        
        config = KDiffConfig(config_dir=self.config_dir)
        
        # Check that environment variables override config values
        self.assertEqual(config.get_config_value("default", "max_messages"), 1000)
        self.assertEqual(config.get_config_value("default", "strategy"), "table")
        
        # Check that other values are still from file
        self.assertEqual(config.get_config_value("default", "checksum_algorithm"), "MD5")
    
    def test_cluster_config_resolution(self):
        """Test that cluster configurations are correctly resolved."""
        config = KDiffConfig(config_dir=self.config_dir)
        
        # Test direct cluster name
        cluster_config = config.get_cluster_config("test-cluster")
        self.assertEqual(cluster_config["bootstrap.servers"], "test-broker:9092")
        self.assertEqual(cluster_config["security.protocol"], "SASL_SSL")
        
        # Test alias resolution
        cluster_config = config.get_cluster_config("test-alias")
        self.assertEqual(cluster_config["bootstrap.servers"], "test-broker:9092")
        self.assertEqual(cluster_config["security.protocol"], "SASL_SSL")
    
    def test_available_clusters(self):
        """Test listing available clusters."""
        config = KDiffConfig(config_dir=self.config_dir)
        clusters = config.get_available_clusters()
        
        self.assertIn("test-cluster", clusters)
        self.assertIn("test-alias", clusters)
    
    def test_invalid_cluster(self):
        """Test error handling for invalid cluster configuration."""
        config = KDiffConfig(config_dir=self.config_dir)
        
        with self.assertRaises(ConfigError):
            config.get_cluster_config("non-existent-cluster")
    
    def test_create_default_configs(self):
        """Test creation of default configuration files."""
        # Create a new empty directory
        with tempfile.TemporaryDirectory() as empty_dir:
            config = KDiffConfig(config_dir=empty_dir)
            config.create_default_configs()
            
            # Check that files were created
            self.assertTrue(os.path.exists(os.path.join(empty_dir, "kdiff.yaml")))
            self.assertTrue(os.path.exists(os.path.join(empty_dir, "clusters.yaml")))
            self.assertTrue(os.path.isdir(os.path.join(empty_dir, "clusters")))


if __name__ == '__main__':
    unittest.main()
