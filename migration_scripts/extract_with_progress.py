"""
Extract with Progress Bar
Enhanced version with tqdm progress bars for better UX
"""

from extract_from_postgres import *
from tqdm import tqdm
import time


def extract_table_with_progress(schema, table, output_dir=None, logger=None):
    """Extract table with progress bar"""
    if logger is None:
        logger = setup_logging()
    
    if output_dir is None:
        output_dir = OUTPUT_DIR / schema
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = Path(output_dir) / f"{schema}_{table}.csv"
    
    print(f"\n{'='*60}")
    print(f"Extracting: {schema}.{table}")
    print(f"{'='*60}")
    
    try:
        conn = get_postgres_connection()
        
        # Get table info first
        info = get_table_info(conn, schema, table)
        
        if info is None:
            print(f"❌ Table {schema}.{table} does not exist")
            return {'success': False, 'error': 'Table does not exist'}
        
        print(f"Table: {info['row_count']:,} rows, {info['col_count']} columns")
        
        # Extract with progress bar
        query = f"SELECT * FROM {schema}.{table}"
        
        print("Extracting data...")
        with tqdm(total=info['row_count'], desc="Reading rows", unit=" rows") as pbar:
            # Use chunksize for progress updates on large tables
            chunks = []
            for chunk in pd.read_sql(query, conn, chunksize=1000):
                chunks.append(chunk)
                pbar.update(len(chunk))
            
            df = pd.concat(chunks, ignore_index=True)
        
        print(f"✅ Extracted {len(df):,} rows")
        
        # Save to CSV with progress
        print("Writing to CSV...")
        with tqdm(total=len(df), desc="Writing CSV", unit=" rows") as pbar:
            # Write in chunks for progress
            df.to_csv(output_file, index=False, encoding='utf-8')
            pbar.update(len(df))
        
        file_size = output_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ Saved to: {output_file}")
        print(f"   Size: {file_size_mb:.2f} MB")
        print(f"{'='*60}\n")
        
        conn.close()
        
        return {
            'success': True,
            'schema': schema,
            'table': table,
            'row_count': len(df),
            'file_path': str(output_file),
            'file_size_mb': file_size_mb
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {'success': False, 'error': str(e)}


if __name__ == '__main__':
    # Test with single table
    result = extract_table_with_progress('raw', 'csv_customers')
    
    if result['success']:
        print("\n🎉 Extraction successful!")
    else:
        print("\n❌ Extraction failed!")
        sys.exit(1)
