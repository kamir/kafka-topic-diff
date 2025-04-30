"""
Topic commands for the KDIFF CLI.

This module provides CLI commands for working with Kafka topics.
"""

import logging
import json
import sys
from typing import Optional, Dict, Any

from kdiff.core.config import KDiffConfig, ConfigError
from kdiff.core.connector import KafkaConnector, KafkaConnectionError
from kdiff.core.reader import TopicReader, TopicReaderError, ReadProgress

logger = logging.getLogger(__name__)


def handle_topic_command(args):
    """
    Handle the 'topic' command.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load configuration
    config_dir = args.config_dir
    kdiff_config = KDiffConfig(config_dir=config_dir)
    
    try:
        # Create Kafka connector
        connector = KafkaConnector(kdiff_config)
        
        # Handle different operations
        if args.list:
            return list_topics(connector, args.cluster)
        elif args.read:
            return read_topic(connector, kdiff_config, args)
        else:
            print("Error: No operation specified. Use --list or --read.")
            return 1
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        print(f"Error: {e}")
        return 1
    except KafkaConnectionError as e:
        logger.error(f"Kafka connection error: {e}")
        print(f"Error connecting to Kafka: {e}")
        return 1
    except Exception as e:
        logger.exception("An error occurred")
        print(f"Error: {e}")
        return 1
    finally:
        # Make sure to close all connections, if connector was created
        if 'connector' in locals():
            connector.close_all_connections()


def list_topics(connector, cluster_name):
    """
    List topics available in a Kafka cluster.
    
    Args:
        connector: The Kafka connector
        cluster_name: Name of the cluster
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        print(f"Fetching topics from cluster '{cluster_name}'...")
        topics = connector.list_topics(cluster_name)
        
        if not topics:
            print(f"No topics found in cluster '{cluster_name}'.")
            return 0
            
        print(f"Topics in cluster '{cluster_name}':")
        for topic in topics:
            print(f"  - {topic}")
            
        print(f"\nTotal topics: {len(topics)}")
        return 0
    except Exception as e:
        logger.exception(f"Error listing topics for cluster {cluster_name}")
        print(f"Error: {e}")
        return 1


def progress_callback(progress: ReadProgress):
    """
    Callback function to report read progress.
    
    Args:
        progress: Read progress information
    """
    sys.stdout.write(f"\r{progress}")
    sys.stdout.flush()


def read_topic(connector, config, args):
    """
    Read messages from a Kafka topic.
    
    Args:
        connector: The Kafka connector
        config: The KDiff configuration
        args: Command-line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        reader = TopicReader(config, connector)
        
        # Set up progress reporting
        if not args.quiet:
            reader.set_progress_callback(progress_callback)
        
        # Parse options
        topic = args.topic
        start_offset = args.start_offset
        end_offset = args.end_offset
        start_timestamp = args.start_timestamp
        end_timestamp = args.end_timestamp
        max_messages = args.max_messages
        partitions = args.partitions.split(',') if args.partitions else None
        if partitions:
            try:
                partitions = [int(p.strip()) for p in partitions]
            except ValueError:
                print(f"Error: Invalid partition list: {args.partitions}. Use comma-separated integers.")
                return 1
        
        format_output = None
        if args.format:
            if args.format == 'json':
                format_output = lambda m: json.dumps({
                    "offset": m.meta.offset,
                    "partition": m.meta.partition,
                    "timestamp": m.meta.timestamp,
                    "key": m.meta.key.decode('utf-8') if m.meta.key else None,
                    "schema_id": m.meta.schema_id,
                    "value": m.content.decode('utf-8', errors='replace')
                })
            elif args.format == 'key-value':
                format_output = lambda m: f"{m.meta.key.decode('utf-8', errors='replace') if m.meta.key else 'null'}: {m.content.decode('utf-8', errors='replace')}"
            elif args.format == 'value':
                format_output = lambda m: m.content.decode('utf-8', errors='replace')
            else:
                format_output = lambda m: (
                    f"Offset: {m.meta.offset}, Partition: {m.meta.partition}, "
                    f"Timestamp: {m.meta.timestamp}, Key: {m.meta.key.decode('utf-8', errors='replace') if m.meta.key else 'null'}, "
                    f"Schema ID: {m.meta.schema_id}\n"
                    f"Value: {m.content.decode('utf-8', errors='replace')}"
                )
        
        # Start reading messages
        print(f"Reading from topic '{topic}' in cluster '{args.cluster}'...")
        
        messages_read = 0
        try:
            for message in reader.read_topic(
                cluster_name=args.cluster,
                topic=topic,
                start_offset=start_offset,
                end_offset=end_offset,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                max_messages=max_messages,
                partitions=partitions
            ):
                messages_read += 1
                
                # Print message based on format
                if format_output:
                    if not args.quiet:
                        print("\n" + format_output(message))
                else:
                    if not args.quiet:
                        print(f"\nPartition: {message.meta.partition}, Offset: {message.meta.offset}")
                        if message.meta.key:
                            print(f"Key: {message.meta.key.decode('utf-8', errors='replace')}")
                        print(f"Timestamp: {message.meta.timestamp}")
                        if message.meta.schema_id:
                            print(f"Schema ID: {message.meta.schema_id}")
                        print("Value:", message.content.decode('utf-8', errors='replace'))
                
                # Break if we've reached the limit
                if max_messages and messages_read >= max_messages:
                    break
        except KeyboardInterrupt:
            print("\nRead operation interrupted.")
        finally:
            # Print final newline after progress
            if not args.quiet:
                print()
        
        # Print summary
        if not args.quiet:
            print(f"Read {messages_read} messages from topic '{topic}'")
        
        return 0
    except TopicReaderError as e:
        logger.error(f"Topic reader error: {e}")
        print(f"Error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Error reading from topic '{args.topic}': {e}")
        print(f"Error: {e}")
        return 1
    finally:
        if 'reader' in locals():
            reader.close()
