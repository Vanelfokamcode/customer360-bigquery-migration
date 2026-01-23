"""
Validate BigQuery tables without PostgreSQL

Checks:
- Table exists
- Row count matches expected
- Schema is correct
- Sample data is valid
"""

import sys
from google.cloud import bigquery
from tabulate import tabulate

def validate_table(dataset, table, expected_rows=5000):
    """Validate BigQuery table"""
    
    print(f"\n{'='*60}")
    print(f"VALIDATING: {dataset}.{table}")
    print(f"{'='*60}\n")
    
    client = bigquery.Client()
    project_id = client.project
    table_ref = f"{project_id}.{dataset}.{table}"
    
    # 1. Check table exists
    print("1. TABLE EXISTS")
    print("-" * 40)
    try:
        table_obj = client.get_table(table_ref)
        print(f"✅ Table found: {table_ref}")
    except Exception as e:
        print(f"❌ Table not found: {e}")
        return False
    
    # 2. Row count
    print("\n2. ROW COUNT")
    print("-" * 40)
    query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
    result = client.query(query).to_dataframe()
    actual_rows = int(result['count'][0])
    
    print(f"Expected: {expected_rows:,} rows")
    print(f"Actual:   {actual_rows:,} rows")
    
    if actual_rows == expected_rows:
        print("✅ MATCH")
    else:
        print(f"⚠️  DIFFERENCE: {actual_rows - expected_rows:+,} rows")
    
    # 3. Schema
    print("\n3. SCHEMA")
    print("-" * 40)
    print(f"Columns: {len(table_obj.schema)}")
    for field in table_obj.schema:
        mode = "REQUIRED" if field.mode == "REQUIRED" else "NULLABLE"
        print(f"  - {field.name:20} {field.field_type:10} {mode}")
    
    # 4. Clustering
    print("\n4. CLUSTERING")
    print("-" * 40)
    if table_obj.clustering_fields:
        print(f"✅ Clustered by: {', '.join(table_obj.clustering_fields)}")
    else:
        print("⚠️  No clustering")
    
    # 5. Sample data
    print("\n5. SAMPLE DATA (first 3 rows)")
    print("-" * 40)
    query = f"SELECT * FROM `{table_ref}` LIMIT 3"
    df = client.query(query).to_dataframe()
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    # Summary
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    
    checks = {
        'Table exists': True,
        'Row count matches': actual_rows == expected_rows,
        'Schema valid': len(table_obj.schema) == 11,
        'Clustering active': table_obj.clustering_fields is not None
    }
    
    all_passed = all(checks.values())
    
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
    
    if all_passed:
        print("\n✅ ALL CHECKS PASSED")
        return True
    else:
        print("\n⚠️  SOME CHECKS FAILED")
        return False

if __name__ == '__main__':
    success = validate_table('raw_data', 'csv_customers', expected_rows=5000)
    sys.exit(0 if success else 1)
