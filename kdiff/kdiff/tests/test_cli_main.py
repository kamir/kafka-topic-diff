"""
Unit tests for the KDIFF command-line interface.
"""

import unittest
from unittest.mock import patch, MagicMock, call
import sys
import os
import io
import argparse

from kdiff.cli.main import main, parse_args, handle_diff_command


class TestCliMain(unittest.TestCase):
    """Tests for the CLI main module."""
    
    def setUp(self):
        """Set up test environment."""
        # Save original sys.argv and stdout
        self.original_argv = sys.argv
        self.original_stdout = sys.stdout
        
    def tearDown(self):
        """Tear down test environment."""
        # Restore original sys.argv and stdout
        sys.argv = self.original_argv
        sys.stdout = self.original_stdout
    
    @patch('sys.stdout', new_callable=io.StringIO)
    @patch('argparse.ArgumentParser.parse_args')
    def test_no_command_error(self, mock_parse_args, mock_stdout):
        """Test error handling when no command is specified."""
        # Create a mock args object with no command
        mock_args = argparse.Namespace(
            command=None,
            verbose=False,
            init_config=False,
            list_clusters=False,
            config_dir=None
        )
        mock_parse_args.return_value = mock_args
        
        # Run main function and check exit code
        exit_code = main()
        self.assertEqual(exit_code, 1)
        
        # Check that error message is displayed
        output = mock_stdout.getvalue()
        self.assertIn("Error: No command specified", output)
    
    @patch('kdiff.cli.main.handle_init_command')
    @patch('argparse.ArgumentParser.parse_args')
    def test_init_command_routing(self, mock_parse_args, mock_handle_init):
        """Test that init command is routed to the correct handler."""
        # Create a mock args object for init command
        mock_args = argparse.Namespace(
            command='init',
            verbose=False,
            init_config=False,
            list_clusters=False,
            config_dir=None
        )
        mock_parse_args.return_value = mock_args
        mock_handle_init.return_value = 0
        
        # Run main function and check exit code
        exit_code = main()
        self.assertEqual(exit_code, 0)
        
        # Check that handler was called with args
        mock_handle_init.assert_called_once_with(mock_args)
    
    @patch('kdiff.cli.main.handle_diff_command')
    @patch('argparse.ArgumentParser.parse_args')
    def test_diff_command_routing(self, mock_parse_args, mock_handle_diff):
        """Test that diff command is routed to the correct handler."""
        # Create a mock args object for diff command
        mock_args = argparse.Namespace(
            command='diff',
            verbose=False,
            init_config=False,
            list_clusters=False,
            config_dir=None,
            topic_a='topic-a',
            topic_b='topic-b',
            cluster_a='cluster-a',
            cluster_b=None,
            start_offset_a=None,
            start_offset_b=None,
            end_offset_a=None,
            end_offset_b=None,
            start_timestamp_a=None,
            start_timestamp_b=None,
            end_timestamp_a=None,
            end_timestamp_b=None,
            max_messages=None,
            compare_content=True,
            compare_offsets=False,
            compare_timestamps=False,
            compare_schema_ids=False,
            strategy=None,
            checksum_algorithm=None,
            output_format=None
        )
        mock_parse_args.return_value = mock_args
        mock_handle_diff.return_value = 0
        
        # Run main function and check exit code
        exit_code = main()
        self.assertEqual(exit_code, 0)
        
        # Check that handler was called with args
        mock_handle_diff.assert_called_once_with(mock_args)
    
    @patch('kdiff.cli.main.handle_topic_command')
    @patch('argparse.ArgumentParser.parse_args')
    def test_topic_command_routing(self, mock_parse_args, mock_handle_topic):
        """Test that topic command is routed to the correct handler."""
        # Create a mock args object for topic command
        mock_args = argparse.Namespace(
            command='topic',
            verbose=False,
            init_config=False,
            list_clusters=False,
            config_dir=None,
            cluster='cluster-a',
            list=True,
            read=False,
            topic=None
        )
        mock_parse_args.return_value = mock_args
        mock_handle_topic.return_value = 0
        
        # Run main function and check exit code
        exit_code = main()
        self.assertEqual(exit_code, 0)
        
        # Check that handler was called with args
        mock_handle_topic.assert_called_once_with(mock_args)


class TestDiffCommand(unittest.TestCase):
    """Tests for the diff command handling."""
    
    @patch('kdiff.core.reporter.DiffReporter')
    @patch('kdiff.core.comparator.StreamComparator')
    @patch('kdiff.core.reader.TopicReader')
    @patch('kdiff.core.connector.KafkaConnector')
    @patch('kdiff.core.cluster_manager.ClusterManager')
    @patch('kdiff.cli.main.KDiffConfig')
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_diff_command_execution(self, mock_stdout, mock_config, mock_manager, 
                                   mock_connector, mock_reader, mock_comparator, 
                                   mock_reporter):
        """Test the execution flow of the diff command."""
        # Create mock objects for dependencies
        mock_config_instance = MagicMock()
        mock_config_instance.get_default_cluster.return_value = 'default-cluster'
        mock_config.return_value = mock_config_instance
        
        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        
        mock_connector_instance = MagicMock()
        mock_connector.return_value = mock_connector_instance
        
        mock_reader_instance = MagicMock()
        mock_messages_a = [MagicMock(), MagicMock()]
        mock_messages_b = [MagicMock(), MagicMock()]
        mock_reader_instance.read_topic.side_effect = [mock_messages_a, mock_messages_b]
        mock_reader.return_value = mock_reader_instance
        
        mock_comparator_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.differences_found = 2
        mock_comparator_instance.compare.return_value = mock_result
        mock_comparator.return_value = mock_comparator_instance
        
        mock_reporter_instance = MagicMock()
        mock_reporter_instance.generate_report.return_value = "Mock Report"
        mock_reporter.return_value = mock_reporter_instance
        
        # Create args
        args = argparse.Namespace(
            config_dir=None,
            verbose=False,
            topic_a='topic-a',
            topic_b='topic-b',
            cluster_a='cluster-a',
            cluster_b=None,
            start_offset_a=None,
            start_offset_b=None,
            end_offset_a=None,
            end_offset_b=None,
            start_timestamp_a=None,
            start_timestamp_b=None,
            end_timestamp_a=None,
            end_timestamp_b=None,
            max_messages=100,
            compare_content=True,
            compare_offsets=False,
            compare_timestamps=False,
            compare_schema_ids=False,
            strategy=None,
            checksum_algorithm=None,
            output_format=None
        )
        
        # Run diff command
        exit_code = handle_diff_command(args)
        
        # Check that correct exit code was returned
        self.assertEqual(exit_code, 2)  # 2 differences found
        
        # Check that the mocks were called correctly
        mock_config.assert_called_once()
        mock_manager.assert_called_once_with(mock_config_instance.clusters_dir, 
                                            os.path.join(mock_config_instance.config_dir, "clusters.yaml"))
        
        mock_connector.assert_any_call(mock_manager_instance.get_cluster_config.return_value)
        self.assertEqual(mock_connector.call_count, 2)
        
        mock_reader.assert_any_call(mock_config_instance, mock_connector_instance)
        self.assertEqual(mock_reader.call_count, 2)
        
        # Check that topic readers were called with correct parameters
        mock_reader_instance.read_topic.assert_any_call('cluster-a', 'topic-a', max_messages=100)
        mock_reader_instance.read_topic.assert_any_call('cluster-a', 'topic-b', max_messages=100)
        
        # Check that comparator.compare was called with the correct topics and messages
        # We don't check the exact options since that's an implementation detail
        self.assertEqual(mock_comparator_instance.compare.call_count, 1)
        call_args = mock_comparator_instance.compare.call_args[0]
        self.assertEqual(call_args[0], 'topic-a')
        self.assertEqual(call_args[1], mock_messages_a)
        self.assertEqual(call_args[2], 'topic-b')
        self.assertEqual(call_args[3], mock_messages_b)
        
        # Check reporter was called correctly
        mock_reporter_instance.generate_report.assert_called_once_with(mock_result)
        
        # Verify output contains the report
        output = mock_stdout.getvalue()
        self.assertIn("Mock Report", output)
        self.assertIn("Differences found: 2", output)
    
    @patch('kdiff.cli.main.KDiffConfig')
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_missing_topics_error(self, mock_stdout, mock_config):
        """Test error handling when topic parameters are missing."""
        # Create mock config
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        
        # Create args with missing topic_a
        args = argparse.Namespace(
            config_dir=None,
            verbose=False,
            topic_a=None,
            topic_b='topic-b',
            cluster_a=None,
            cluster_b=None
        )
        
        # Run diff command
        exit_code = handle_diff_command(args)
        
        # Check that error code was returned
        self.assertEqual(exit_code, 1)
        
        # Check that error message is displayed
        output = mock_stdout.getvalue()
        self.assertIn("Error: Both --topic-a and --topic-b are required", output)


if __name__ == '__main__':
    unittest.main()
