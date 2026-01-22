"""
Extract data from PostgreSQL to CSV files

This script extracts tables from PostgreSQL and saves them as CSV files
for later import into BigQuery.

Usage:
    python extract_from_postgres.py --table raw.csv_customers
    python extract_from_postgres.py --all  # Extract all tables
"""

import psycopg2
import pandas as pd
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'customer360'),
    'user': os.getenv('POSTGRES_USER', 'dataeng'),
    'password': os.getenv('POSTGRES_PASSWORD', '2003')
}

# Output directory
OUTPUT_DIR = Path('data/exports')
LOG_DIR = Path('logs')

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Tables to extract (schema.table_name)
TABLES_TO_EXTRACT = {
    'raw': ['csv_customers', 'csv_orders', 'csv_products'],
    'staging': ['stg_csv_customers', 'stg_csv_orders', 'stg_csv_products'],
    'warehouse': ['dim_customers', 'customer_rfm', 'customer_health', 'cohort_retention']
}


def setup_logging():
    """Setup logging to file and console"""
    import logging
    
    # Create log file with timestamp
    log_file = LOG_DIR / f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)


def get_postgres_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        print(f"\nConnection parameters:")
        print(f"  Host: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
        print(f"  Database: {POSTGRES_CONFIG['database']}")
        print(f"  User: {POSTGRES_CONFIG['user']}")
        print(f"\nPlease check your .env file")
        sys.exit(1)


def get_table_info(conn, schema, table):
    """Get basic info about a table"""
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        )
    """, (schema, table))
    
    exists = cursor.fetchone()[0]
    
    if not exists:
        return None
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    row_count = cursor.fetchone()[0]
    
    # Get column count
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_schema = %s 
        AND table_name = %s
    """, (schema, table))
    
    col_count = cursor.fetchone()[0]
    
    cursor.close()
    
    return {
        'row_count': row_count,
        'col_count': col_count
    }


def extract_table(schema, table, output_dir=None, logger=None):
    """
    Extract a single table from PostgreSQL to CSV
    
    Args:
        schema: Schema name (e.g., 'raw', 'staging', 'warehouse')
        table: Table name (e.g., 'csv_customers')
        output_dir: Output directory (default: data/exports/{schema})
        logger: Logger instance
    
    Returns:
        dict: Extraction results (success, row_count, file_path, file_size)
    """
    if logger is None:
        logger = setup_logging()
    
    if output_dir is None:
        output_dir = OUTPUT_DIR / schema
    
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Output file path
    output_file = Path(output_dir) / f"{schema}_{table}.csv"
    
    logger.info(f"{'='*60}")
    logger.info(f"Extracting: {schema}.{table}")
    logger.info(f"{'='*60}")
    
    try:
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        
        # Get table info
        info = get_table_info(conn, schema, table)
        
        if info is None:
            logger.error(f"❌ Table {schema}.{table} does not exist")
            return {
                'success': False,
                'schema': schema,
                'table': table,
                'error': 'Table does not exist'
            }
        
        logger.info(f"Table info: {info['row_count']:,} rows, {info['col_count']} columns")
        
        # Extract data
        query = f"SELECT * FROM {schema}.{table}"
        logger.info(f"Executing query: {query}")
        
        # Use pandas to read SQL → CSV (handles types automatically)
        df = pd.read_sql(query, conn)
        
        logger.info(f"✅ Extracted {len(df):,} rows to DataFrame")
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        # Get file size
        file_size = output_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"✅ Saved to: {output_file}")
        logger.info(f"   File size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
        logger.info(f"   Rows: {len(df):,}")
        logger.info(f"   Columns: {len(df.columns)}")
        
        # Close connection
        conn.close()
        
        logger.info(f"{'='*60}")
        logger.info(f"✅ SUCCESS: {schema}.{table}")
        logger.info(f"{'='*60}\n")
        
        return {
            'success': True,
            'schema': schema,
            'table': table,
            'row_count': len(df),
            'col_count': len(df.columns),
            'file_path': str(output_file),
            'file_size_bytes': file_size,
            'file_size_mb': file_size_mb
        }
        
    except Exception as e:
        logger.error(f"❌ FAILED: {schema}.{table}")
        logger.error(f"   Error: {str(e)}")
        logger.error(f"{'='*60}\n")
        
        return {
            'success': False,
            'schema': schema,
            'table': table,
            'error': str(e)
        }


def extract_all_tables(logger=None):
    """Extract all tables defined in TABLES_TO_EXTRACT"""
    if logger is None:
        logger = setup_logging()
    
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING ALL TABLES")
    logger.info("="*60 + "\n")
    
    results = []
    
    for schema, tables in TABLES_TO_EXTRACT.items():
        for table in tables:
            result = extract_table(schema, table, logger=logger)
            results.append(result)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("="*60)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    logger.info(f"\nTotal tables: {len(results)}")
    logger.info(f"✅ Successful: {len(successful)}")
    logger.info(f"❌ Failed: {len(failed)}")
    
    if successful:
        logger.info("\n✅ Successfully extracted:")
        total_rows = 0
        total_size = 0
        
        for r in successful:
            logger.info(f"   {r['schema']}.{r['table']:25} → {r['row_count']:>8,} rows, {r['file_size_mb']:>6.2f} MB")
            total_rows += r['row_count']
            total_size += r['file_size_mb']
        
        logger.info(f"\n   TOTAL: {total_rows:,} rows, {total_size:.2f} MB")
    
    if failed:
        logger.info("\n❌ Failed extractions:")
        for r in failed:
            logger.info(f"   {r['schema']}.{r['table']:25} → {r.get('error', 'Unknown error')}")
    
    logger.info("\n" + "="*60)
    
    return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract PostgreSQL tables to CSV files'
    )
    parser.add_argument(
        '--table',
        help='Table to extract in format schema.table (e.g., raw.csv_customers)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Extract all tables'
    )
    parser.add_argument(
        '--schema',
        help='Extract all tables from a specific schema'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("\n" + "="*60)
    logger.info("PostgreSQL to CSV Extraction Tool")
    logger.info("="*60)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Output directory: {OUTPUT_DIR.absolute()}")
    logger.info("="*60 + "\n")
    
    # Extract based on arguments
    if args.all:
        # Extract all tables
        results = extract_all_tables(logger)
        
    elif args.schema:
        # Extract all tables from specific schema
        if args.schema not in TABLES_TO_EXTRACT:
            logger.error(f"❌ Unknown schema: {args.schema}")
            logger.error(f"   Available schemas: {list(TABLES_TO_EXTRACT.keys())}")
            sys.exit(1)
        
        results = []
        for table in TABLES_TO_EXTRACT[args.schema]:
            result = extract_table(args.schema, table, logger=logger)
            results.append(result)
        
    elif args.table:
        # Extract single table
        if '.' not in args.table:
            logger.error("❌ Table must be in format schema.table")
            logger.error("   Example: raw.csv_customers")
            sys.exit(1)
        
        schema, table = args.table.split('.', 1)
        result = extract_table(schema, table, logger=logger)
        results = [result]
        
    else:
        # No arguments - show help
        parser.print_help()
        sys.exit(0)
    
    # Exit code based on results
    if all(r['success'] for r in results):
        logger.info("\n✅ All extractions completed successfully")
        sys.exit(0)
    else:
        logger.error("\n❌ Some extractions failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
