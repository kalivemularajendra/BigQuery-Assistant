# BigQuery FastMCP Server

A FastMCP-powered BigQuery server implementation that provides intelligent data discovery and analytics capabilities through specialized AI agents. This package includes both standalone FastMCP server functionality and ADK Web Interface compatibility.

## Features

- **Multi-Agent Architecture**: Specialized agents for data discovery and analytics
- **FastMCP Integration**: Modern HTTP/SSE transport for scalable web applications
- **ADK Web Compatibility**: Works seamlessly with Google ADK Web Interface
- **BigQuery Operations**: Full support for querying, schema discovery, and dataset management
- **Sample Data Creation**: Built-in tools for creating test datasets and sample data

## Architecture

### Agent System

The package implements a multi-agent orchestration system:

1. **Orchestrator Agent**: Routes user requests to appropriate specialized agents
2. **Data Discovery Agent**: Handles schema exploration, data cataloging, and structure analysis
3. **Data Analytics Agent**: Performs statistical analysis, business intelligence, and insights generation

### Transport Options

- **FastMCP Server** (`server.py`): HTTP/SSE transport for standalone web applications
- **ADK Compatible Agent** (`agent.py`): stdio-based transport for ADK Web Interface integration

## Installation

1. **Clone the repository and install dependencies:**
```bash
pip install fastmcp google-cloud-bigquery python-dotenv google-adk
```

2. **Set up environment variables in `.env` file:**
```env
BIGQUERY_PROJECT=your-project-id
BIGQUERY_LOCATION=your-location  # e.g., asia-south1, US
BIGQUERY_KEY_FILE=/path/to/service-account-key.json  # Optional
```

3. **Configure BigQuery Authentication:**
   - **Option 1**: Use service account key file (set `BIGQUERY_KEY_FILE`)
   - **Option 2**: Use Application Default Credentials (ADC)
   - **Option 3**: Use gcloud authentication

## Usage

### ADK Web Interface (Recommended)

For use with Google ADK Web Interface, the agent is automatically configured:

```python
from bigquery_fastmcp import agent

# The root_agent is ready to use with ADK Web Interface
# It automatically handles routing between discovery and analytics agents
```

The ADK agent provides:
- Intelligent request routing between specialized agents
- Comprehensive BigQuery operations
- Web-optimized performance and error handling

### Standalone FastMCP Server

For standalone web applications or direct HTTP/SSE access:

```bash
# Start the FastMCP server
python bigquery_fastmcp/server.py --project YOUR_PROJECT --location YOUR_LOCATION --port 8001

# Server runs on http://127.0.0.1:8001 by default
# SSE endpoint available at http://127.0.0.1:8001/sse/
```

**Server Options:**
```bash
python server.py --help

optional arguments:
  --project PROJECT     BigQuery project ID
  --location LOCATION   BigQuery location (default: US)
  --key-file KEY_FILE   Path to service account key file
  --host HOST           Host to run server on (default: localhost)
  --port PORT           Port to run server on (default: 8001)
```

## Agent Capabilities

### Data Discovery Agent
Specializes in:
- **Data Catalog Management**: Systematic exploration of available datasets and tables
- **Schema Analysis**: Deep dive into table structures, column types, and constraints
- **Data Profiling**: Analysis of data distribution and quality assessment
- **Relationship Discovery**: Finding connections between tables
- **Metadata Extraction**: Comprehensive documentation of data assets

**Example Queries:**
- "What tables are available in the project?"
- "Describe the schema of the customers table"
- "Show me the structure of the sales dataset"

### Data Analytics Agent
Specializes in:
- **Statistical Analysis**: Comprehensive statistical analysis and distributions
- **Business Intelligence**: KPI calculations and business metrics
- **Trend Analysis**: Pattern identification and anomaly detection
- **Comparative Analysis**: Segment and period comparisons
- **Data Aggregation**: Meaningful summaries for decision making

**Example Queries:**
- "Analyze sales trends over the last quarter"
- "What is the average order value by region?"
- "Compare user engagement between different cohorts"

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `BIGQUERY_PROJECT` | BigQuery project ID | None | Yes |
| `BIGQUERY_LOCATION` | BigQuery location/region | `US` | No |
| `BIGQUERY_KEY_FILE` | Service account key file path | None | No |
| `HOST` | Server host (FastMCP only) | `localhost` | No |
| `PORT` | Server port (FastMCP only) | `8001` | No |

### BigQuery Authentication

The server supports multiple authentication methods:

1. **Service Account Key File** (Recommended for production):
   ```env
   BIGQUERY_KEY_FILE=/path/to/service-account-key.json
   ```

2. **Application Default Credentials**:
   ```bash
   gcloud auth application-default login
   ```

3. **Compute Engine/Cloud Shell**: Automatically uses attached service account

## Sample Data Creation

The server includes utilities for creating sample datasets for testing:

```python
# Create a complete sample environment
create_complete_sample("test_dataset", "asia-south1")
```

This creates:
- A new BigQuery dataset
- Sample `departments` and `employees` tables
- Populated with 10 departments and 50 employees

## Logging

The FastMCP server logs to both stdout and file:
- **Log file**: `mcp_bigquery_fastmcp_server.log`
- **Log level**: DEBUG (configurable)
- **Log format**: Timestamp, logger name, level, message

## Error Handling

The package includes comprehensive error handling for:
- BigQuery authentication failures
- Invalid queries and malformed SQL
- Network connectivity issues
- Missing or invalid configuration
- Table/dataset access permissions

## Development

### Project Structure
```
bigquery_fastmcp/
├── __init__.py          # Package initialization
├── agent.py             # ADK Web Interface compatible agent
├── server.py            # FastMCP HTTP/SSE server
├── config.py            # Configuration management
└── README.md            # This file
```

### Extending the Server

To add new tools, modify `server.py`:

```python
@mcp.tool()
def your_new_tool(param: str) -> str:
    """Description of your new tool"""
    # Implementation here
    return result
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify service account key file path
   - Check that service account has BigQuery permissions
   - Try `gcloud auth application-default login`

2. **Connection Issues**:
   - Verify project ID is correct
   - Check network connectivity to BigQuery
   - Ensure location/region is valid

3. **Permission Errors**:
   - Verify service account has required BigQuery roles:
     - `BigQuery Data Editor`
     - `BigQuery Job User`
     - `BigQuery Data Viewer`

### Debugging

Enable detailed logging by setting the log level:
```python
import logging
logging.getLogger('mcp_bigquery_fastmcp_server').setLevel(logging.DEBUG)
```

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section above
- Review BigQuery documentation
- File an issue in the repository
