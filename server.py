from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
import logging
from typing import Any, Optional, List
import os
import random
import uuid
from fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "mcp_bigquery_fastmcp_server.log")

# Configure a module-level logger with one stdout handler and an optional file handler
logger = logging.getLogger('mcp_bigquery_fastmcp_server')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # Only add a file handler if a path is configured
    try:
        file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        logger.debug("Could not open log file for writing. Continuing without file logging.")

logger.setLevel(logging.INFO)

class BigQueryDatabase:
    def __init__(self, project: str, location: str, key_file: Optional[str]):
        """Create a BigQuery client for the given project and location.

        Key file paths are not logged for security reasons; only presence is noted.
        Supports both service account key files and Application Default Credentials (ADC).
        """
        logger.info(f"Initializing BigQuery client for project: {project}, location: {location}")
        if not project:
            raise ValueError("Project is required")
        if not location:
            raise ValueError("Location is required")
        
        credentials = None
        
        if key_file and os.path.exists(key_file):
            # Try to load as service account key file first
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    key_file,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                logger.info("Using service account credentials from key file")
            except Exception as e:
                # If it fails, it might be an ADC file - fall back to default credentials
                logger.info("Key file is not a service account file, falling back to Application Default Credentials")
                credentials = None
        
        if credentials is None:
            # Use Application Default Credentials
            try:
                credentials, adc_project = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                logger.info("Using Application Default Credentials")
            except Exception as e:
                logger.error(f"Failed to load any credentials: {e}")
                raise ValueError("Could not load credentials. Please run 'gcloud auth application-default login' or provide a valid service account key file.") from e

        self.client = bigquery.Client(credentials=credentials, project=project, location=location)

    def execute_query(self, query: str, params: Optional[List[bigquery.ScalarQueryParameter]] = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return the results as a list of dictionaries.

        `params` should be a list of `bigquery.ScalarQueryParameter` when provided.
        """
        logger.debug("Executing SQL query")
        try:
            if params:
                job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
            else:
                job = self.client.query(query)
                
            results = job.result()
            rows = [dict(row.items()) for row in results]
            logger.debug(f"Query returned {len(rows)} rows")
            return rows
        except Exception:
            logger.exception("Error executing query")
            raise
    
    def list_tables(self) -> list[str]:
        """Return a list of all <dataset>.<table> entries in the project."""
        datasets = list(self.client.list_datasets())
        tables: list[str] = []
        for dataset in datasets:
            for table in self.client.list_tables(dataset.dataset_id):
                tables.append(f"{dataset.dataset_id}.{table.table_id}")
        return tables

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        """Return the DDL for a table using INFORMATION_SCHEMA.TABLES."""

        parts = table_name.split(".")
        if len(parts) != 2 and len(parts) != 3:
            raise ValueError(f"Invalid table name: {table_name}")

        dataset_id = ".".join(parts[:-1])
        table_id = parts[-1]

        query = f"""
            SELECT ddl
            FROM {dataset_id}.INFORMATION_SCHEMA.TABLES
            WHERE table_name = @table_name;
        """
        return self.execute_query(query, params=[bigquery.ScalarQueryParameter("table_name", "STRING", table_id)])

    def create_dataset(self, dataset_name: str, location: Optional[str] = None) -> str:
        """Create a new dataset and return a short status message."""
        
        dataset_ref = self.client.dataset(dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location or "US"
        
        try:
            self.client.create_dataset(dataset)
            return f"Dataset {dataset_name} created"
        except Exception as e:
            if "Already Exists" in str(e):
                return f"Dataset {dataset_name} already exists"
            raise

    def create_sample_tables(self, dataset_name: str) -> str:
        """Create sample `departments` and `employees` tables (schemas enforced)."""
        
        # Create departments table
        departments_schema = [
            bigquery.SchemaField("dept_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dept_name", "STRING", mode="REQUIRED")
        ]
        
        dept_table_ref = self.client.dataset(dataset_name).table("departments")
        dept_table = bigquery.Table(dept_table_ref, schema=departments_schema)
        
        # Create employees table
        employees_schema = [
            bigquery.SchemaField("emp_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("emp_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dept_id", "STRING", mode="REQUIRED")
        ]
        
        emp_table_ref = self.client.dataset(dataset_name).table("employees")
        emp_table = bigquery.Table(emp_table_ref, schema=employees_schema)
        
        try:
            self.client.create_table(dept_table)
            self.client.create_table(emp_table)
            return "Sample tables created"
        except Exception:
            raise

    def insert_sample_data(self, dataset_name: str) -> str:
        """Populate sample `departments` and `employees` tables with generated rows."""
        
        # Insert departments
        departments = [
            {"dept_id": f"dept_{i}", "dept_name": f"Department_{i}"}
            for i in range(1, 11)
        ]
        
        errors = self.client.insert_rows_json(f"{self.client.project}.{dataset_name}.departments", departments)
        if errors:
            raise RuntimeError(f"Failed to insert departments: {errors}")
        
        # Insert employees
        employees = []
        for i in range(1, 51):
            dept_id = random.choice(departments)["dept_id"]
            emp = {
                "emp_id": f"emp_{uuid.uuid4().hex[:8]}",
                "emp_name": f"Employee_{i}",
                "dept_id": dept_id
            }
            employees.append(emp)
        
        errors = self.client.insert_rows_json(f"{self.client.project}.{dataset_name}.employees", employees)
        if errors:
            raise RuntimeError(f"Failed to insert employees: {errors}")
        
        return "Sample data inserted (10 departments, 50 employees)"

    def create_complete_sample(self, dataset_name: str, location: Optional[str] = None) -> str:
        """Create dataset, sample tables, and insert data; return summary text."""
        
        result = []
        
        # Step 1: Create dataset
        dataset_result = self.create_dataset(dataset_name, location)
        result.append(dataset_result)

        # Step 2: Create tables
        result.append(self.create_sample_tables(dataset_name))

        # Step 3: Insert data
        result.append(self.insert_sample_data(dataset_name))
        
        return "\n".join(result)

# Initialize the database connection (will be set in main)
db: Optional[BigQueryDatabase] = None

# Initialize FastMCP server
mcp = FastMCP("BigQuery_FastMCP_Server")

@mcp.tool()
def execute_query(query: str) -> str:
    """Execute a SELECT query on the BigQuery database"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return str(db.execute_query(query))
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def list_tables() -> str:
    """List all tables in the BigQuery database"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return str(db.list_tables())
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def describe_table(table_name: str) -> str:
    """Get the schema information for a specific table"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return str(db.describe_table(table_name))
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def create_dataset(dataset_name: str, location: str = "US") -> str:
    """Create a new BigQuery dataset"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return db.create_dataset(dataset_name, location)
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def create_sample_tables(dataset_name: str) -> str:
    """Create sample departments and employees tables in a dataset"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return db.create_sample_tables(dataset_name)
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def insert_sample_data(dataset_name: str) -> str:
    """Insert sample data into departments and employees tables"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return db.insert_sample_data(dataset_name)
    except Exception as e:
        return f"Error: {e}"

@mcp.tool()
def create_complete_sample(dataset_name: str, location: str = "asia-south1") -> str:
    """Create dataset, sample tables, and insert data in one step"""
    if not db:
        return "Error: Database not initialized"
    
    try:
        return db.create_complete_sample(dataset_name, location)
    except Exception as e:
        return f"Error: {e}"

def main(project: str, location: str, key_file: Optional[str], host: str = "127.0.0.1", port: int = 8000):
    """Main function to start the FastMCP server with SSE"""
    global db
    
    logger.info("Starting FastMCP BigQuery Server")
    
    # Initialize database connection
    db = BigQueryDatabase(project, location, key_file)
    
    # Run FastMCP server with SSE transport
    mcp.run(transport="sse", host=host, port=port)

if __name__ == "__main__":
    import argparse
    
    # Get environment variables with defaults
    project = os.getenv('BIGQUERY_PROJECT')
    location = os.getenv('BIGQUERY_LOCATION', 'US')
    key_file = os.getenv('BIGQUERY_KEY_FILE')
    host = os.getenv('HOST', 'localhost')
    port = int(os.getenv('PORT', '8000'))
    
    parser = argparse.ArgumentParser(description='BigQuery FastMCP Server with SSE')
    parser.add_argument('--project', default=project, help='BigQuery project ID')
    parser.add_argument('--location', default=location, help='BigQuery location')
    parser.add_argument('--key-file', default=key_file, help='Path to service account key file')
    parser.add_argument('--host', default=host, help='Host to run the server on')
    parser.add_argument('--port', type=int, default=port, help='Port to run the server on')
    
    args = parser.parse_args()
    
    # Ensure we have a project ID
    if not args.project:
        logger.error("Project is required. Provide BIGQUERY_PROJECT environment variable or use --project argument")
        exit(1)
    
    main(args.project, args.location, args.key_file, args.host, args.port)
