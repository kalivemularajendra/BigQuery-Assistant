

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class BigQueryConfig:
    """Simple BigQuery configuration helper"""
    
    def __init__(self, project_id=None, location=None, key_file=None):
        self.project_id = project_id or os.getenv('BIGQUERY_PROJECT')
        self.location = location or os.getenv('BIGQUERY_LOCATION')
        self.key_file = key_file or os.getenv('BIGQUERY_KEY_FILE')
    
    def get_server_args(self):
        """Get command line arguments for the MCP server"""
        args = ["--project", self.project_id, "--location", self.location]
        
        if self.key_file:
            args.extend(["--key-file", self.key_file])
        
        return args
    
    def validate(self):
        """Validate required configuration"""
        if not self.project_id:
            raise ValueError("BigQuery project ID is required")
        return True


def get_config():
    """Get default BigQuery configuration"""
    return BigQueryConfig()
