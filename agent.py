"""BigQuery FastMCP Agent for ADK Web Interface.

This provides a lightweight orchestrator agent and sub-agents for data discovery
and analytics. For standalone HTTP/SSE server usage, run server.py.
"""

from pathlib import Path
from dotenv import load_dotenv
import logging

from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, SseConnectionParams

from .config import get_config

load_dotenv()

# Use the server in the same package
PATH_TO_BIGQUERY_MCP_SERVER_SCRIPT = str((Path(__file__).parent / "server.py").resolve())

# Setup logger for this module
logger = logging.getLogger("bigquery_fastmcp_agent")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Load BigQuery configuration with a simple fallback
try:
    bigquery_config = get_config()
    bigquery_config.validate()
    server_args = bigquery_config.get_server_args()
    logger.info("BigQuery configuration loaded")
except Exception as e:
    logger.warning("BigQuery config warning: using fallback values")
    class FallbackConfig:
        def get_server_args(self):
            return ["--project", "default-project", "--location", "US"]
    bigquery_config = FallbackConfig()
    server_args = bigquery_config.get_server_args()

# Agent-specific prompts
DATA_DISCOVERY_PROMPT = (
    "You are a Data Discovery Agent: explore datasets, describe schemas, and sample data. "
    "Use available tools (list-tables, describe-table, execute-query) and summarize findings."
)

DATA_ANALYTICS_PROMPT = (
    "You are a Data Analytics Agent: run analytical SQL queries, summarize distributions, KPIs, and trends. "
    "Start with schema and sampling, then generate aggregated insights."
)

# Orchestrator prompt for task routing
BIGQUERY_PROMPT = (
    "You are an orchestrator. Route stat/analytics requests to Data Analytics Agent and discovery requests to Data Discovery Agent. "
    "If unsure, ask for clarification and report which agent you use."
)

# Create specialized agents
data_discovery_agent = LlmAgent(
    model="gemini-2.0-flash",
    name="BigQuery_Data_Discovery_Agent",
    instruction=DATA_DISCOVERY_PROMPT,
    tools=[
        MCPToolset(
            connection_params=SseConnectionParams(
                url="http://127.0.0.1:8001/sse/"
            ),
        ),
    ],
)

data_analytics_agent = LlmAgent(
    model="gemini-2.0-flash",
    name="BigQuery_Data_Analytics_Agent", 
    instruction=DATA_ANALYTICS_PROMPT,
    tools=[
        MCPToolset(
            connection_params=SseConnectionParams(
                url="http://127.0.0.1:8001/sse/"
            ),
        ),
    ],
)


# Multi-agent setup with specialized agents
root_agent = LlmAgent(
    name="orchestrator", 
    model="gemini-2.0-flash",
    instruction= BIGQUERY_PROMPT,
    description="An orchestrator agent that routes user requests to specialized sub-agents for data discovery and data analytics in BigQuery.",
    sub_agents=[
        data_discovery_agent,
        data_analytics_agent,
    ]
)