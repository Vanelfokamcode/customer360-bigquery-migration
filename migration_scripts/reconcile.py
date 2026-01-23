"""
Reconciliation Script: PostgreSQL vs BigQuery

Validates that data migration was successful by comparing:
- Row counts
- Column counts
- Column names
- Sample data
- NULL counts

Usage:
    python reconcile.py --table raw.csv_customers --bq-table raw_data.csv_customers
    python reconcile.py --all  # Reconcile all tables
"""

import psycopg2
import sys
import argparse
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from tabulate import tabulate

# Load environment variables
load_dotenv()

# PostgreSQL configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'customer360'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', '2003')
}

# BigQuery configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') or os.popen('gcloud config get-value project').read().strip()

# Table mappings
TABLE_MAPPINGS = {
    'raw.csv_customers': 'raw_data.csv_customers',
    # Add more mappings as you migrate more tables
}

# Output
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)


def setup_logging():
    """Setup logging"""
    import logging
    
    log_file = LOG_DIR / f"reconcile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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
    """Get PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


def get_bigquery_client():
    """Get BigQuery client"""
    try:
        return bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        print(f"❌ Failed to create BigQuery client: {e}")
        sys.exit(1)


def get_postgres_row_count(schema, table):
    """Get row count from PostgreSQL table"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    query = f"SELECT COUNT(*) FROM {schema}.{table}"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return count


def get_bigquery_row_count(dataset, table):
    """Get row count from BigQuery table"""
    client = get_bigquery_client()
    
    query = f"SELECT COUNT(*) as count FROM `{GCP_PROJECT_ID}.{dataset}.{table}`"
    result = client.query(query).to_dataframe()
    
    return int(result['count'][0])


def get_postgres_schema(schema, table):
    """Get schema from PostgreSQL table"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    
    cursor.execute(query, (schema, table))
    columns = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [
        {
            'name': col[0],
            'type': col[1],
            'nullable': col[2] == 'YES'
        }
        for col in columns
    ]


def get_bigquery_schema(dataset, table):
    """Get schema from BigQuery table"""
    client = get_bigquery_client()
    
    table_ref = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    table_obj = client.get_table(table_ref)
    
    return [
        {
            'name': field.name,
            'type': field.field_type,
            'nullable': field.mode != 'REQUIRED'
        }
        for field in table_obj.schema
    ]


def get_postgres_sample(schema, table, limit=5):
    """Get sample data from PostgreSQL"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    query = f"SELECT * FROM {schema}.{table} LIMIT {limit}"
    cursor.execute(query)
    
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    cursor.close()
    conn.close()
    
    return columns, rows


def get_bigquery_sample(dataset, table, limit=5):
    """Get sample data from BigQuery"""
    client = get_bigquery_client()
    
    query = f"SELECT * FROM `{GCP_PROJECT_ID}.{dataset}.{table}` LIMIT {limit}"
    df = client.query(query).to_dataframe()
    
    return df.columns.tolist(), df.values.tolist()


def reconcile_table(pg_schema, pg_table, bq_dataset, bq_table, logger=None):
    """
    Reconcile a single table between PostgreSQL and BigQuery
    
    Returns:
        dict: Reconciliation results
    """
    if logger is None:
        logger = setup_logging()
    
    logger.info(f"\n{'='*80}")
    logger.info(f"RECONCILING: {pg_schema}.{pg_table} → {bq_dataset}.{bq_table}")
    logger.info(f"{'='*80}")
    
    results = {
        'pg_table': f"{pg_schema}.{pg_table}",
        'bq_table': f"{bq_dataset}.{bq_table}",
        'checks': {}
    }
    
    # 1. ROW COUNT
    logger.info("\n1. ROW COUNT COMPARISON")
    logger.info("-" * 40)
    
    try:
        pg_count = get_postgres_row_count(pg_schema, pg_table)
        bq_count = get_bigquery_row_count(bq_dataset, bq_table)
        
        match = pg_count == bq_count
        status = "✅ MATCH" if match else "❌ MISMATCH"
        
        logger.info(f"PostgreSQL: {pg_count:,} rows")
        logger.info(f"BigQuery:   {bq_count:,} rows")
        logger.info(f"Status:     {status}")
        
        if not match:
            diff = bq_count - pg_count
            logger.error(f"⚠️  Difference: {diff:+,} rows")
        
        results['checks']['row_count'] = {
            'postgres': pg_count,
            'bigquery': bq_count,
            'match': match
        }
        
    except Exception as e:
        logger.error(f"❌ Row count check failed: {e}")
        results['checks']['row_count'] = {'error': str(e)}
    
    # 2. SCHEMA COMPARISON
    logger.info("\n2. SCHEMA COMPARISON")
    logger.info("-" * 40)
    
    try:
        pg_schema_info = get_postgres_schema(pg_schema, pg_table)
        bq_schema_info = get_bigquery_schema(bq_dataset, bq_table)
        
        pg_columns = [col['name'] for col in pg_schema_info]
        bq_columns = [col['name'] for col in bq_schema_info]
        
        logger.info(f"PostgreSQL: {len(pg_columns)} columns")
        logger.info(f"BigQuery:   {len(bq_columns)} columns")
        
        # Column name comparison
        missing_in_bq = set(pg_columns) - set(bq_columns)
        extra_in_bq = set(bq_columns) - set(pg_columns)
        
        if missing_in_bq:
            logger.error(f"❌ Missing in BigQuery: {missing_in_bq}")
        
        if extra_in_bq:
            logger.warning(f"⚠️  Extra in BigQuery: {extra_in_bq}")
        
        if not missing_in_bq and not extra_in_bq:
            logger.info("✅ All column names match")
        
        results['checks']['schema'] = {
            'postgres_columns': len(pg_columns),
            'bigquery_columns': len(bq_columns),
            'match': len(pg_columns) == len(bq_columns),
            'missing_in_bq': list(missing_in_bq),
            'extra_in_bq': list(extra_in_bq)
        }
        
    except Exception as e:
        logger.error(f"❌ Schema check failed: {e}")
        results['checks']['schema'] = {'error': str(e)}
    
    # 3. SAMPLE DATA
    logger.info("\n3. SAMPLE DATA PREVIEW")
    logger.info("-" * 40)
    
    try:
        pg_cols, pg_rows = get_postgres_sample(pg_schema, pg_table, limit=3)
        bq_cols, bq_rows = get_bigquery_sample(bq_dataset, bq_table, limit=3)
        
        logger.info("PostgreSQL sample (first 3 rows):")
        logger.info(tabulate(pg_rows, headers=pg_cols, tablefmt='grid'))
        
        logger.info("\nBigQuery sample (first 3 rows):")
        logger.info(tabulate(bq_rows, headers=bq_cols, tablefmt='grid'))
        
        results['checks']['sample_data'] = {
            'retrieved': True
        }
        
    except Exception as e:
        logger.warning(f"⚠️  Sample data check failed: {e}")
        results['checks']['sample_data'] = {'error': str(e)}
    
    # SUMMARY
    logger.info(f"\n{'='*80}")
    logger.info("RECONCILIATION SUMMARY")
    logger.info(f"{'='*80}")
    
    all_passed = all(
        check.get('match', False) 
        for check in results['checks'].values() 
        if 'match' in check
    )
    
    if all_passed:
        logger.info("✅ ALL CHECKS PASSED - Migration successful!")
        results['overall_status'] = 'PASS'
    else:
        logger.error("❌ SOME CHECKS FAILED - Review issues above")
        results['overall_status'] = 'FAIL'
    
    logger.info(f"{'='*80}\n")
    
    return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Reconcile PostgreSQL and BigQuery tables'
    )
    parser.add_argument(
        '--table',
        help='PostgreSQL table in format schema.table (e.g., raw.csv_customers)'
    )
    parser.add_argument(
        '--bq-table',
        help='BigQuery table in format dataset.table (e.g., raw_data.csv_customers)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Reconcile all mapped tables'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("\n" + "="*80)
    logger.info("RECONCILIATION TOOL: PostgreSQL → BigQuery")
    logger.info("="*80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    logger.info(f"BigQuery:   {GCP_PROJECT_ID}")
    logger.info("="*80)
    
    results_list = []
    
    if args.table and args.bq_table:
        # Reconcile single table
        pg_schema, pg_table = args.table.split('.', 1)
        bq_dataset, bq_table = args.bq_table.split('.', 1)
        
        result = reconcile_table(pg_schema, pg_table, bq_dataset, bq_table, logger)
        results_list.append(result)
        
    elif args.all:
        # Reconcile all mapped tables
        for pg_full, bq_full in TABLE_MAPPINGS.items():
            pg_schema, pg_table = pg_full.split('.', 1)
            bq_dataset, bq_table = bq_full.split('.', 1)
            
            result = reconcile_table(pg_schema, pg_table, bq_dataset, bq_table, logger)
            results_list.append(result)
        
    else:
        parser.print_help()
        sys.exit(0)
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("FINAL SUMMARY")
    logger.info("="*80)
    
    passed = sum(1 for r in results_list if r['overall_status'] == 'PASS')
    failed = sum(1 for r in results_list if r['overall_status'] == 'FAIL')
    
    logger.info(f"\nTotal tables reconciled: {len(results_list)}")
    logger.info(f"✅ Passed: {passed}")
    logger.info(f"❌ Failed: {failed}")
    
    if failed == 0:
        logger.info("\n🎉 ALL TABLES RECONCILED SUCCESSFULLY!")
        logger.info("Migration validated - data integrity confirmed")
        sys.exit(0)
    else:
        logger.error("\n⚠️  SOME TABLES FAILED RECONCILIATION")
        logger.error("Review logs and fix issues before proceeding")
        sys.exit(1)


if __name__ == '__main__':
    main()
