"""
Constants for the KDIFF tool.

This module contains constant values used throughout the KDIFF tool.
"""

import os

# Configuration file locations
DEFAULT_KTOOLS_DIR = "~/ktools"
DEFAULT_CLUSTERS_DIR = os.path.join(DEFAULT_KTOOLS_DIR, "clusters")
DEFAULT_KDIFF_CONFIG = os.path.join(DEFAULT_KTOOLS_DIR, "kdiff.yaml")
DEFAULT_CLUSTERS_CONFIG = os.path.join(DEFAULT_KTOOLS_DIR, "clusters.yaml")

# Environment variable prefixes
ENV_PREFIX = "KDIFF_"
ENV_CONFIG_DIR = f"{ENV_PREFIX}CONFIG_DIR"
ENV_CLUSTER_A = f"{ENV_PREFIX}CLUSTER_A"
ENV_CLUSTER_B = f"{ENV_PREFIX}CLUSTER_B"

# Default configuration values
DEFAULT_CONFIG = {
    "default": {
        "checksum_algorithm": "SHA256",
        "output_format": "text",
        "strategy": "stream",
        "comparison": {
            "content": True,
            "offsets": False,
            "timestamps": False,
            "schema_ids": False
        },
        "max_messages": 1000
    },
    "service": {
        "host": "0.0.0.0",
        "port": 8080,
        "workers": 4,
        "log_level": "info",
        "allow_origins": ["http://localhost:8080"]
    }
}

# Default clusters configuration
DEFAULT_CLUSTERS_CONFIG_CONTENT = {
    "clusters": {}
}

# Default Kafka client properties
DEFAULT_KAFKA_PROPS = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "PLAINTEXT"
}
