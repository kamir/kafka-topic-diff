"""
Command-line interface entry point for KDIFF.

This module provides the main entry point for the command-line version of KDIFF.
"""

import sys
import os
import logging
import argparse

from kdiff.core.config import KDiffConfig, ConfigError
from kdiff.utils.file_utils import expand_path
from kdiff.cli.config_commands import handle_config_cluster_command
from kdiff.cli.topic_commands import handle_topic_command

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare messages between two Kafka topics."
    )
    
    # Common options
    parser.add_argument('--config-dir', help='Path to configuration directory (default: ~/ktools)')
    parser.add_argument('--verbose', action='store_true', help='Increase output verbosity')
    
    # Create subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # diff command (main comparison functionality)
    diff_parser = subparsers.add_parser('diff', help='Compare Kafka topics', description='Compare Kafka topics')
    
    # Cluster options for diff
    diff_parser.add_argument('--cluster-a', help='Name of cluster A')
    diff_parser.add_argument('--cluster-b', help='Name of cluster B (defaults to cluster-a)')
    
    # Topic options for diff
    diff_parser.add_argument('--topic-a', help='Name of topic A')
    diff_parser.add_argument('--topic-b', help='Name of topic B')
    
    # Range options for diff
    diff_parser.add_argument('--start-offset-a', type=int, help='Start offset for topic A')
    diff_parser.add_argument('--start-offset-b', type=int, help='Start offset for topic B')
    diff_parser.add_argument('--end-offset-a', type=int, help='End offset for topic A')
    diff_parser.add_argument('--end-offset-b', type=int, help='End offset for topic B')
    diff_parser.add_argument('--start-timestamp-a', type=int, help='Start timestamp for topic A (unix ms)')
    diff_parser.add_argument('--start-timestamp-b', type=int, help='Start timestamp for topic B (unix ms)')
    diff_parser.add_argument('--end-timestamp-a', type=int, help='End timestamp for topic A (unix ms)')
    diff_parser.add_argument('--end-timestamp-b', type=int, help='End timestamp for topic B (unix ms)')
    diff_parser.add_argument('--max-messages', type=int, help='Maximum messages to compare')
    
    # Comparison options for diff
    diff_parser.add_argument('--compare-content', action='store_true', help='Compare message content')
    diff_parser.add_argument('--compare-offsets', action='store_true', help='Compare message offsets')
    diff_parser.add_argument('--compare-timestamps', action='store_true', help='Compare message timestamps')
    diff_parser.add_argument('--compare-schema-ids', action='store_true', help='Compare schema IDs')
    diff_parser.add_argument('--strategy', choices=['stream', 'table'], help='Comparison strategy')
    diff_parser.add_argument('--checksum-algorithm', help='Algorithm for checksums')
    diff_parser.add_argument('--output-format', choices=['text', 'json'], help='Output format')
    
    # initialize command
    init_parser = subparsers.add_parser('init', help='Initialize configuration')
    
    # topic command for working with Kafka topics
    topic_parser = subparsers.add_parser('topic', help='Kafka topic operations', description='Kafka topic operations')
    topic_parser.add_argument('--cluster', required=True, help='Name of the cluster to connect to')
    topic_parser.add_argument('--list', action='store_true', help='List available topics in the cluster')
    
    # Topic read subcommand options
    topic_parser.add_argument('--read', action='store_true', help='Read messages from a topic')
    topic_parser.add_argument('--topic', help='Topic name to read from')
    topic_parser.add_argument('--start-offset', type=int, help='Start offset (inclusive)')
    topic_parser.add_argument('--end-offset', type=int, help='End offset (exclusive)')
    topic_parser.add_argument('--start-timestamp', type=int, help='Start timestamp in ms since epoch (inclusive)')
    topic_parser.add_argument('--end-timestamp', type=int, help='End timestamp in ms since epoch (exclusive)')
    topic_parser.add_argument('--max-messages', type=int, help='Maximum number of messages to read')
    topic_parser.add_argument('--partitions', help='Comma-separated list of partitions to read from')
    topic_parser.add_argument('--format', choices=['json', 'key-value', 'value', 'full'], 
                            help='Output format for messages')
    topic_parser.add_argument('--quiet', action='store_true', help='Suppress progress output')
    
    # config command with its own subcommands
    config_parser = subparsers.add_parser('config', help='Configuration commands', description='Configuration commands')
    config_subparsers = config_parser.add_subparsers(dest='config_command', help='Configuration subcommand')
    
    # config cluster subcommand
    cluster_parser = config_subparsers.add_parser('cluster', help='Manage cluster configurations')
    cluster_group = cluster_parser.add_mutually_exclusive_group(required=True)
    cluster_group.add_argument('--add', metavar='PROPERTIES_FILE', help='Add a cluster from properties file')
    cluster_group.add_argument('--list', action='store_true', help='List configured clusters')
    cluster_group.add_argument('--remove', metavar='CLUSTER_NAME', help='Remove a cluster configuration')
    cluster_parser.add_argument('--name', help='Name to assign to the cluster (default: derived from filename)')
    cluster_parser.add_argument('--force', action='store_true', help='Force overwrite if cluster exists')
    
    # Maintain backward compatibility with the old format
    parser.add_argument('--init-config', action='store_true', help='Create default configuration files')
    parser.add_argument('--list-clusters', action='store_true', help='List available clusters')
    
    return parser.parse_args()


def list_clusters(config):
    """List available clusters."""
    clusters = config.get_available_clusters()
    if not clusters:
        print("No clusters configured.")
        return
    
    print("Available clusters:")
    for cluster in clusters:
        print(f"  - {cluster}")


def handle_init_command(args):
    """
    Handle the 'init' command.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Create configuration
        config_dir = expand_path(args.config_dir) if args.config_dir else None
        config = KDiffConfig(config_dir=config_dir)
        
        # Initialize configuration files
        config.create_default_configs()
        print(f"Default configuration files created in: {config.config_dir}")
        return 0
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        return 1


def handle_diff_command(args):
    """
    Handle the 'diff' command.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Create configuration
        config_dir = expand_path(args.config_dir) if args.config_dir else None
        config = KDiffConfig(config_dir=config_dir)
        
        # Validate required parameters
        if not args.topic_a or not args.topic_b:
            print("Error: Both --topic-a and --topic-b are required for comparison.")
            return 1
        
        # Determine cluster names
        cluster_a_name = args.cluster_a or config.get_default_cluster()
        cluster_b_name = args.cluster_b or cluster_a_name
        
        if not cluster_a_name:
            print("Error: No cluster specified and no default cluster configured.")
            return 1
            
        # Set up logging based on verbosity
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            
        print("KDIFF - Kafka Topic Diff Tool")
        print("-----------------------------")
        print(f"Config directory: {config.config_dir}")
        print(f"Cluster A: {cluster_a_name}")
        print(f"Cluster B: {cluster_b_name}")
        print(f"Topic A: {args.topic_a}")
        print(f"Topic B: {args.topic_b}")
        
        # Import needed modules here to avoid circular imports
        from kdiff.core.cluster_manager import ClusterManager
        from kdiff.core.connector import KafkaConnector
        from kdiff.core.reader import TopicReader
        from kdiff.core.comparator import StreamComparator, ComparisonOptions, ChecksumAlgorithm
        from kdiff.core.reporter import DiffReporter, ReportOptions, ReportFormat
        
        # Create cluster manager and connect to clusters
        manager = ClusterManager(config.clusters_dir, os.path.join(config.config_dir, "clusters.yaml"))
        
        # Connect to clusters
        print(f"Connecting to clusters...")
        connector_a = KafkaConnector(manager.get_cluster_config(cluster_a_name))
        connector_b = KafkaConnector(manager.get_cluster_config(cluster_b_name))
        
        # Create topic readers
        reader_a = TopicReader(config, connector_a)
        reader_b = TopicReader(config, connector_b)
        
        # Prepare reading parameters
        read_params_a = {
            'start_offset': args.start_offset_a,
            'end_offset': args.end_offset_a,
            'start_timestamp': args.start_timestamp_a,
            'end_timestamp': args.end_timestamp_a,
        }
        
        read_params_b = {
            'start_offset': args.start_offset_b,
            'end_offset': args.end_offset_b,
            'start_timestamp': args.start_timestamp_b,
            'end_timestamp': args.end_timestamp_b,
        }
        
        # Apply max_messages if provided
        if args.max_messages:
            read_params_a['max_messages'] = args.max_messages
            read_params_b['max_messages'] = args.max_messages
            
        # Create comparison options
        # Note: By default, we compare content unless other options are specified
        compare_content = True
        if args.compare_offsets or args.compare_timestamps or args.compare_schema_ids:
            # If any specific comparison is requested, only do those
            compare_content = args.compare_content
            
        comp_options = ComparisonOptions(
            compare_content=compare_content,
            compare_offsets=args.compare_offsets,
            compare_timestamps=args.compare_timestamps,
            compare_schema_ids=args.compare_schema_ids,
        )
        
        # Set checksum algorithm if provided
        if args.checksum_algorithm:
            try:
                comp_options.checksum_algorithm = ChecksumAlgorithm(args.checksum_algorithm.upper())
            except ValueError:
                print(f"Error: Invalid checksum algorithm: {args.checksum_algorithm}")
                print(f"Valid options are: {', '.join([a.value.lower() for a in ChecksumAlgorithm])}")
                return 1
        
        # Create comparator based on strategy
        if args.strategy and args.strategy.lower() == 'table':
            # Use table comparison strategy (unordered matching)
            from kdiff.core.comparator import TableComparator
            comparator = TableComparator(options=comp_options)
            print(f"Using table comparison strategy (unordered matching)")
        else:
            # Default to stream comparison (ordered matching)
            comparator = StreamComparator(options=comp_options)
            print(f"Using stream comparison strategy (ordered matching)")
            
        # Create reporter options
        report_options = ReportOptions(
            format=ReportFormat.JSON if args.output_format == 'json' else ReportFormat.TEXT,
            verbosity=2 if args.verbose else 1,  # Default to normal, verbose mode uses detailed
        )
        
        reporter = DiffReporter(options=report_options)
        
        # Read messages
        print(f"Reading messages from {args.topic_a} on {cluster_a_name}...")
        messages_a = reader_a.read_topic(cluster_a_name, args.topic_a, **{k: v for k, v in read_params_a.items() if v is not None})
        
        print(f"Reading messages from {args.topic_b} on {cluster_b_name}...")
        messages_b = reader_b.read_topic(cluster_b_name, args.topic_b, **{k: v for k, v in read_params_b.items() if v is not None})
        
        # Perform comparison
        print("Comparing messages...")
        result = comparator.compare(
            args.topic_a, messages_a,
            args.topic_b, messages_b,
            options=comp_options
        )
        
        # Generate and print report
        print("\nComparison Results:")
        print(reporter.generate_report(result))
        
        # Return with exit code based on differences found
        if result.differences_found > 0:
            print(f"\nDifferences found: {result.differences_found}")
            return 2  # Use exit code 2 to indicate differences were found
        else:
            print("\nNo differences found.")
            return 0
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        return 1


def main():
    """Main entry point for the KDIFF CLI."""
    try:
        args = parse_args()
        
        # Set logging level based on verbosity
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Handle backward compatibility commands
        if args.init_config:
            return handle_init_command(args)
            
        if args.list_clusters:
            # Create configuration and list clusters
            config_dir = expand_path(args.config_dir) if args.config_dir else None
            config = KDiffConfig(config_dir=config_dir)
            list_clusters(config)
            return 0
        
        # Handle commands
        if args.command == 'init':
            return handle_init_command(args)
        elif args.command == 'diff':
            return handle_diff_command(args)
        elif args.command == 'topic':
            return handle_topic_command(args)
        elif args.command == 'config':
            if args.config_command == 'cluster':
                return handle_config_cluster_command(args)
            else:
                print("Error: Unknown config subcommand.")
                return 1
        elif not args.command:
            print("Error: No command specified. Use 'diff', 'init', 'topic', 'config', or --help for more info.")
            return 1
        else:
            print(f"Error: Unknown command: {args.command}")
            return 1
        
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
