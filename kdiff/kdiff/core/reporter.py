"""
Reporter module for KDIFF.

This module provides functionality to generate reports from comparison results.
"""

import logging
import json
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from enum import Enum, auto
from dataclasses import dataclass

from kdiff.core.comparator import ComparisonResult, MessageDifference

logger = logging.getLogger(__name__)


class ReportFormat(Enum):
    """Supported report formats."""
    TEXT = "text"
    JSON = "json"


class DiffSeverity(Enum):
    """Severity levels for differences."""
    INFO = auto()      # Informational differences, not critical
    WARNING = auto()   # Potential issues worth attention
    ERROR = auto()     # Serious differences that require investigation
    CRITICAL = auto()  # Critical differences that must be addressed


@dataclass
class ReportOptions:
    """Options for report generation."""
    
    format: ReportFormat = ReportFormat.TEXT
    verbosity: int = 1  # 0=minimal, 1=normal, 2=detailed, 3=debug
    max_diff_details: Optional[int] = None  # Maximum number of detailed differences to include
    include_content_diffs: bool = True
    include_offset_diffs: bool = True
    include_timestamp_diffs: bool = True
    include_schema_id_diffs: bool = True
    include_missing_messages: bool = True
    severity_threshold: DiffSeverity = DiffSeverity.INFO  # Minimum severity to include


class DiffReporter:
    """
    Reporter for comparison results.
    
    This class handles processing comparison results and generating reports
    in different formats with configurable detail levels.
    """
    
    def __init__(self, options: Optional[ReportOptions] = None):
        """
        Initialize the diff reporter.
        
        Args:
            options: Report options
        """
        self.options = options or ReportOptions()
    
    def _get_severity(self, difference: MessageDifference) -> DiffSeverity:
        """
        Determine the severity of a difference.
        
        Args:
            difference: The difference to evaluate
            
        Returns:
            Severity level
        """
        # Missing messages are considered high severity
        if difference.is_missing:
            return DiffSeverity.ERROR
            
        # Different severities based on difference type
        if difference.difference_type == "content":
            return DiffSeverity.ERROR
        elif difference.difference_type == "schema_id":
            return DiffSeverity.WARNING
        elif difference.difference_type == "offset":
            return DiffSeverity.INFO
        elif difference.difference_type == "timestamp":
            return DiffSeverity.INFO
        
        # Default severity
        return DiffSeverity.INFO
    
    def _filter_differences(self, differences: List[MessageDifference]) -> List[MessageDifference]:
        """
        Filter differences based on report options.
        
        Args:
            differences: List of differences to filter
            
        Returns:
            Filtered list of differences
        """
        result = []
        
        for diff in differences:
            # Apply severity filter
            severity = self._get_severity(diff)
            if severity.value < self.options.severity_threshold.value:
                continue
                
            # Apply type filters
            if diff.is_missing and not self.options.include_missing_messages:
                continue
                
            if not diff.is_missing:
                if diff.difference_type == "content" and not self.options.include_content_diffs:
                    continue
                if diff.difference_type == "offset" and not self.options.include_offset_diffs:
                    continue
                if diff.difference_type == "timestamp" and not self.options.include_timestamp_diffs:
                    continue
                if diff.difference_type == "schema_id" and not self.options.include_schema_id_diffs:
                    continue
            
            result.append(diff)
            
            # Apply max details limit
            if self.options.max_diff_details is not None and len(result) >= self.options.max_diff_details:
                break
        
        return result
    
    def _generate_text_report(self, result: ComparisonResult) -> str:
        """
        Generate a text report from comparison results.
        
        Args:
            result: Comparison result
            
        Returns:
            Text report
        """
        lines = []
        
        # Report header
        lines.append("=" * 80)
        lines.append(f"KDIFF COMPARISON REPORT: {result.topic_a} vs {result.topic_b}")
        lines.append("=" * 80)
        lines.append("")
        
        # Summary statistics
        lines.append("SUMMARY:")
        lines.append(f"  Messages compared: {result.messages_compared}")
        lines.append(f"  Differences found: {result.differences_found}")
        
        # More detailed summary for verbosity > 0
        if self.options.verbosity > 0:
            if result.missing_in_a > 0:
                lines.append(f"  Missing in {result.topic_a}: {result.missing_in_a}")
            if result.missing_in_b > 0:
                lines.append(f"  Missing in {result.topic_b}: {result.missing_in_b}")
            if result.content_differences > 0:
                lines.append(f"  Content differences: {result.content_differences}")
            if result.offset_differences > 0:
                lines.append(f"  Offset differences: {result.offset_differences}")
            if result.timestamp_differences > 0:
                lines.append(f"  Timestamp differences: {result.timestamp_differences}")
            if result.schema_id_differences > 0:
                lines.append(f"  Schema ID differences: {result.schema_id_differences}")
                
            lines.append("")
            lines.append(f"  Processing time: {result.elapsed_seconds:.2f} seconds")
            lines.append(f"  Processing rate: {result.messages_per_second:.2f} messages/second")
            lines.append("")
        
        # Comparison options
        if self.options.verbosity >= 2:
            lines.append("COMPARISON OPTIONS:")
            lines.append(f"  Compare content: {result.options.compare_content}")
            lines.append(f"  Compare offsets: {result.options.compare_offsets}")
            lines.append(f"  Compare timestamps: {result.options.compare_timestamps}")
            lines.append(f"  Compare schema IDs: {result.options.compare_schema_ids}")
            
            if result.options.checksum_algorithm:
                lines.append(f"  Checksum algorithm: {result.options.checksum_algorithm.value}")
                
            if result.options.timestamp_tolerance_ms > 0:
                lines.append(f"  Timestamp tolerance: {result.options.timestamp_tolerance_ms} ms")
                
            lines.append("")
        
        # Filter differences based on options
        filtered_diffs = self._filter_differences(result.differences)
        
        # Detailed differences
        if filtered_diffs:
            lines.append("DIFFERENCES:")
            
            for i, diff in enumerate(filtered_diffs):
                severity = self._get_severity(diff)
                
                # Add separator between differences for higher verbosity
                if i > 0 and self.options.verbosity >= 2:
                    lines.append("-" * 40)
                
                if diff.is_missing:
                    # Missing message
                    message = diff.message_a if diff.message_b is None else diff.message_b
                    missing_in = diff.topic_a if diff.message_a is None else diff.topic_b
                    
                    lines.append(f"  {severity.name}: Message at index {diff.index} missing in {missing_in}")
                    
                    if self.options.verbosity >= 2 and message:
                        lines.append(f"    Partition: {message.meta.partition}, Offset: {message.meta.offset}")
                        lines.append(f"    Timestamp: {message.meta.timestamp}")
                        
                        if message.meta.key:
                            try:
                                key_str = message.meta.key.decode('utf-8', errors='replace')
                                lines.append(f"    Key: {key_str}")
                            except Exception:
                                lines.append(f"    Key: {message.meta.key}")
                        
                        if self.options.verbosity >= 3:
                            # Add content preview for high verbosity
                            try:
                                content_preview = message.content[:100].decode('utf-8', errors='replace')
                                lines.append(f"    Content preview: {content_preview}...")
                            except Exception:
                                # For binary data, show hex representation
                                content_preview = message.content[:50].hex()
                                lines.append(f"    Content preview (hex): {content_preview}...")
                else:
                    # Content, offset, timestamp, or schema_id difference
                    lines.append(f"  {severity.name}: {diff.difference_type.capitalize()} difference at index {diff.index}")
                    lines.append(f"    Details: {diff.details}")
                    
                    if self.options.verbosity >= 2:
                        # Add message metadata for higher verbosity
                        if diff.message_a and diff.message_b:
                            lines.append(f"    {diff.topic_a} offset: {diff.message_a.meta.offset}, {diff.topic_b} offset: {diff.message_b.meta.offset}")
                            lines.append(f"    {diff.topic_a} timestamp: {diff.message_a.meta.timestamp}, {diff.topic_b} timestamp: {diff.message_b.meta.timestamp}")
                            
                            if diff.message_a.meta.key or diff.message_b.meta.key:
                                key_a = diff.message_a.meta.key.decode('utf-8', errors='replace') if diff.message_a.meta.key else "None"
                                key_b = diff.message_b.meta.key.decode('utf-8', errors='replace') if diff.message_b.meta.key else "None"
                                lines.append(f"    {diff.topic_a} key: {key_a}, {diff.topic_b} key: {key_b}")
        else:
            if result.differences_found > 0:
                lines.append("DIFFERENCES: All differences filtered based on current options")
            else:
                lines.append("DIFFERENCES: None found")
        
        # Report footer
        lines.append("")
        lines.append("=" * 80)
        lines.append("")
        
        return "\n".join(lines)
    
    def _generate_json_report(self, result: ComparisonResult) -> str:
        """
        Generate a JSON report from comparison results.
        
        Args:
            result: Comparison result
            
        Returns:
            JSON report as string
        """
        # Create report structure
        report = {
            "report_type": "kdiff_comparison",
            "timestamp": int(time.time()),
            "comparison": {
                "topic_a": result.topic_a,
                "topic_b": result.topic_b,
                "messages_compared": result.messages_compared,
                "differences_found": result.differences_found,
                "missing_in_a": result.missing_in_a,
                "missing_in_b": result.missing_in_b,
                "content_differences": result.content_differences,
                "offset_differences": result.offset_differences,
                "timestamp_differences": result.timestamp_differences,
                "schema_id_differences": result.schema_id_differences,
                "elapsed_seconds": result.elapsed_seconds,
                "messages_per_second": result.messages_per_second
            },
            "options": {
                "compare_content": result.options.compare_content,
                "compare_offsets": result.options.compare_offsets,
                "compare_timestamps": result.options.compare_timestamps,
                "compare_schema_ids": result.options.compare_schema_ids,
                "checksum_algorithm": result.options.checksum_algorithm.value if result.options.checksum_algorithm else None,
                "timestamp_tolerance_ms": result.options.timestamp_tolerance_ms
            }
        }
        
        # Filter differences based on options
        filtered_diffs = self._filter_differences(result.differences)
        
        # Convert differences to JSON-serializable format
        if filtered_diffs:
            diff_list = []
            
            for diff in filtered_diffs:
                diff_dict = {
                    "index": diff.index,
                    "type": diff.difference_type,
                    "severity": self._get_severity(diff).name,
                    "details": diff.details,
                    "is_missing": diff.is_missing
                }
                
                # Add message details if available and verbosity allows
                if self.options.verbosity >= 2:
                    if diff.message_a:
                        diff_dict["message_a"] = {
                            "offset": diff.message_a.meta.offset,
                            "partition": diff.message_a.meta.partition,
                            "timestamp": diff.message_a.meta.timestamp,
                            "schema_id": diff.message_a.meta.schema_id
                        }
                        
                        if diff.message_a.meta.key:
                            try:
                                diff_dict["message_a"]["key"] = diff.message_a.meta.key.decode('utf-8', errors='replace')
                            except Exception:
                                diff_dict["message_a"]["key"] = None
                    
                    if diff.message_b:
                        diff_dict["message_b"] = {
                            "offset": diff.message_b.meta.offset,
                            "partition": diff.message_b.meta.partition,
                            "timestamp": diff.message_b.meta.timestamp,
                            "schema_id": diff.message_b.meta.schema_id
                        }
                        
                        if diff.message_b.meta.key:
                            try:
                                diff_dict["message_b"]["key"] = diff.message_b.meta.key.decode('utf-8', errors='replace')
                            except Exception:
                                diff_dict["message_b"]["key"] = None
                
                # Add content preview for high verbosity
                if self.options.verbosity >= 3:
                    if diff.message_a and not diff.is_missing:
                        try:
                            diff_dict["message_a"]["content_preview"] = diff.message_a.content[:100].decode('utf-8', errors='replace')
                        except Exception:
                            diff_dict["message_a"]["content_preview_hex"] = diff.message_a.content[:50].hex()
                    
                    if diff.message_b and not diff.is_missing:
                        try:
                            diff_dict["message_b"]["content_preview"] = diff.message_b.content[:100].decode('utf-8', errors='replace')
                        except Exception:
                            diff_dict["message_b"]["content_preview_hex"] = diff.message_b.content[:50].hex()
                
                diff_list.append(diff_dict)
            
            report["differences"] = diff_list
            
            # Include counts by severity
            severity_counts = {}
            for diff in filtered_diffs:
                severity = self._get_severity(diff).name
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            report["severity_counts"] = severity_counts
        
        # Convert to JSON string
        return json.dumps(report, indent=2)
    
    def generate_report(self, result: ComparisonResult) -> str:
        """
        Generate a report from comparison results.
        
        Args:
            result: Comparison result
            
        Returns:
            Report as string
        """
        try:
            if self.options.format == ReportFormat.JSON:
                return self._generate_json_report(result)
            else:
                return self._generate_text_report(result)
        except Exception as e:
            logger.exception(f"Error generating report: {e}")
            return f"Error generating report: {e}"
    
    def write_report_to_file(self, result: ComparisonResult, file_path: str) -> None:
        """
        Write a report to a file.
        
        Args:
            result: Comparison result
            file_path: Path to write the report to
            
        Raises:
            IOError: If the file cannot be written
        """
        report = self.generate_report(result)
        
        with open(file_path, 'w') as f:
            f.write(report)
        
        logger.info(f"Report written to {file_path}")
