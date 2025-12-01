"""
FastMCP BigQuery Server Package

This package contains a BigQuery MCP server implementation using FastMCP with SSE transport.
"""

# Import the agent for ADK compatibility
from . import agent

# Avoid circular imports - don't import from server module
__all__ = ['server', 'config', 'agent']
