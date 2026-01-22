"""
Load CSV files to BigQuery

This script loads CSV files (extracted from PostgreSQL) into BigQuery tables.

Usage:
    python load_to_bigquery.py --file data/exports/raw/raw_csv_customers.csv --table raw_data.csv_customers
    python load_to_bigquery.py --schema raw  # Load all CSV files from schema
    python load_to_bigquery.py --all  # Load all CSV files
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
if not GCP_PROJECT_ID:
    # Try to get from gcloud config
    import subprocess
    try:
        result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'project'],
            capture_output=True,
            text=True,
            check=True
        )
        GCP_PROJECT_ID = result.stdout.strip()
    except:
        print("❌ GCP_PROJECT_ID not found in .env or gcloud config")
        sys.exit(1)

# Directories
EXPORTS_DIR = Path('data/exports')
LOG_DIR = Path('logs')

# Schema mapping: PostgreSQL schema → BigQuery dataset
SCHEMA_MAPPING = {
    'raw': 'raw_data',
    'staging': 'staging',
    'warehouse': 'analytics'
}


def setup_logging():
    """Setup logging"""
    import logging
    
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)


def get_bigquery_client():
    """Create BigQuery client"""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        return client
    except Exception as e:
        print(f"❌ Failed to create BigQuery client: {e}")
        print("\nTroubleshooting:")
        print("1. Run: gcloud auth application-default login")
        print("2. Verify project: gcloud config get-value project")
        print("3. Check .env file has GCP_PROJECT_ID")
        sys.exit(1)


def dataset_exists(client, dataset_id):
    """Check if dataset exists"""
    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def load_csv_to_bigquery(
    csv_path,
    dataset_id,
    table_id,
    project_id=None,
    write_disposition='WRITE_TRUNCATE',
    logger=None
):
    """
    Load CSV file to BigQuery table
    
    Args:
        csv_path: Path to CSV file
        dataset_id: BigQuery dataset ID (e.g., 'raw_data')
        table_id: BigQuery table ID (e.g., 'csv_customers')
        project_id: GCP project ID (default: from config)
        write_disposition: WRITE_TRUNCATE (overwrite) or WRITE_APPEND
        logger: Logger instance
    
    Returns:
        dict: Load results
    """
    if logger is None:
        logger = setup_logging()
    
    if project_id is None:
        project_id = GCP_PROJECT_ID
    
    csv_path = Path(csv_path)
    
    logger.info(f"{'='*60}")
    logger.info(f"Loading: {csv_path.name}")
    logger.info(f"{'='*60}")
    
    # Check if CSV exists
    if not csv_path.exists():
        logger.error(f"❌ CSV file not found: {csv_path}")
        return {
            'success': False,
            'error': 'File not found'
        }
    
    # Get file size
    file_size = csv_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    logger.info(f"CSV file: {csv_path}")
    logger.info(f"File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    try:
        # Create BigQuery client
        client = get_bigquery_client()
        
        # Check if dataset exists
        if not dataset_exists(client, dataset_id):
            logger.error(f"❌ Dataset {dataset_id} does not exist")
            logger.error(f"   Create it first: bq mk --dataset {project_id}:{dataset_id}")
            return {
                'success': False,
                'error': f'Dataset {dataset_id} not found'
            }
        
        # Define table reference
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        logger.info(f"Target table: {table_ref}")
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,      # Auto-detect schema from CSV
            write_disposition=write_disposition,  # Overwrite or append
            allow_quoted_newlines=True,  # Handle newlines in quoted fields
            allow_jagged_rows=False,  # Fail if row has wrong number of columns
            max_bad_records=0  # Fail on any bad record
        )
        
        logger.info(f"Load configuration:")
        logger.info(f"  - Source format: CSV")
        logger.info(f"  - Skip header: Yes")
        logger.info(f"  - Schema detection: Auto")
        logger.info(f"  - Write mode: {write_disposition}")
        
        # Load CSV
        logger.info(f"Starting load job...")
        
        with open(csv_path, 'rb') as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
        
        # Wait for job to complete
        logger.info(f"Job ID: {load_job.job_id}")
        logger.info(f"Waiting for job to complete...")
        
        load_job.result()  # Wait for completion
        
        # Get table info
        table = client.get_table(table_ref)
        
        logger.info(f"✅ Load completed successfully!")
        logger.info(f"   Rows loaded: {table.num_rows:,}")
        logger.info(f"   Schema fields: {len(table.schema)}")
        logger.info(f"   Table size: {table.num_bytes / (1024*1024):.2f} MB")
        logger.info(f"   Created: {table.created}")
        logger.info(f"   Modified: {table.modified}")
        
        # Show schema
        logger.info(f"\nTable schema:")
        for field in table.schema:
            logger.info(f"   - {field.name:30} {field.field_type}")
        
        logger.info(f"{'='*60}")
        logger.info(f"✅ SUCCESS: {table_ref}")
        logger.info(f"{'='*60}\n")
        
        return {
            'success': True,
            'table_ref': table_ref,
            'rows_loaded': table.num_rows,
            'schema_fields': len(table.schema),
            'table_size_mb': table.num_bytes / (1024*1024)
        }
        
    except Exception as e:
        logger.error(f"❌ Load failed: {str(e)}")
        logger.error(f"{'='*60}\n")
        
        return {
            'success': False,
            'error': str(e)
        }


def load_schema_csvs(schema, logger=None):
    """Load all CSV files from a PostgreSQL schema"""
    if logger is None:
        logger = setup_logging()
    
    # Get BigQuery dataset name
    dataset_id = SCHEMA_MAPPING.get(schema)
    if not dataset_id:
        logger.error(f"❌ Unknown schema: {schema}")
        logger.error(f"   Available: {list(SCHEMA_MAPPING.keys())}")
        return []
    
    # Find CSV files
    csv_dir = EXPORTS_DIR / schema
    if not csv_dir.exists():
        logger.error(f"❌ Directory not found: {csv_dir}")
        return []
    
    csv_files = list(csv_dir.glob('*.csv'))
    
    if not csv_files:
        logger.warning(f"⚠️  No CSV files found in {csv_dir}")
        return []
    
    logger.info(f"\nFound {len(csv_files)} CSV file(s) in {schema}/")
    
    results = []
    
    for csv_file in csv_files:
        # Extract table name from filename
        # Format: {schema}_{table}.csv → table
        filename = csv_file.stem  # raw_csv_customers
        if filename.startswith(f"{schema}_"):
            table_id = filename[len(schema)+1:]  # csv_customers
        else:
            table_id = filename
        
        result = load_csv_to_bigquery(
            csv_path=csv_file,
            dataset_id=dataset_id,
            table_id=table_id,
            logger=logger
        )
        
        results.append(result)
    
    return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Load CSV files to BigQuery'
    )
    parser.add_argument(
        '--file',
        help='CSV file to load'
    )
    parser.add_argument(
        '--table',
        help='Target table in format dataset.table (e.g., raw_data.csv_customers)'
    )
    parser.add_argument(
        '--schema',
        help='Load all CSV files from a PostgreSQL schema (raw, staging, warehouse)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Load all CSV files from all schemas'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("\n" + "="*60)
    logger.info("CSV to BigQuery Load Tool")
    logger.info("="*60)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"GCP Project: {GCP_PROJECT_ID}")
    logger.info("="*60 + "\n")
    
    results = []
    
    if args.file and args.table:
        # Load single file to specific table
        if '.' not in args.table:
            logger.error("❌ Table must be in format dataset.table")
            logger.error("   Example: raw_data.csv_customers")
            sys.exit(1)
        
        dataset_id, table_id = args.table.split('.', 1)
        
        result = load_csv_to_bigquery(
            csv_path=args.file,
            dataset_id=dataset_id,
            table_id=table_id,
            logger=logger
        )
        
        results = [result]
        
    elif args.schema:
        # Load all CSV files from schema
        results = load_schema_csvs(args.schema, logger=logger)
        
    elif args.all:
        # Load all CSV files from all schemas
        for schema in SCHEMA_MAPPING.keys():
            schema_results = load_schema_csvs(schema, logger=logger)
            results.extend(schema_results)
        
    else:
        # No arguments - show help
        parser.print_help()
        sys.exit(0)
    
    # Summary
    if results:
        logger.info("\n" + "="*60)
        logger.info("LOAD SUMMARY")
        logger.info("="*60)
        
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]
        
        logger.info(f"\nTotal files: {len(results)}")
        logger.info(f"✅ Successful: {len(successful)}")
        logger.info(f"❌ Failed: {len(failed)}")
        
        if successful:
            logger.info("\n✅ Successfully loaded:")
            total_rows = 0
            
            for r in successful:
                logger.info(f"   {r['table_ref']:50} → {r['rows_loaded']:>8,} rows")
                total_rows += r['rows_loaded']
            
            logger.info(f"\n   TOTAL: {total_rows:,} rows")
        
        if failed:
            logger.info("\n❌ Failed loads:")
            for r in failed:
                logger.info(f"   {r.get('error', 'Unknown error')}")
        
        logger.info("\n" + "="*60)
        
        # Exit code
        if all(r['success'] for r in results):
            logger.info("\n✅ All loads completed successfully")
            sys.exit(0)
        else:
            logger.error("\n❌ Some loads failed")
            sys.exit(1)


if __name__ == '__main__':
    main()
