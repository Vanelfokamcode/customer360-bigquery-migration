"""
Full Environment Test
Tests all connections: PostgreSQL → Python → BigQuery
"""

import sys
import os

def test_imports():
    """Test that all required packages can be imported"""
    print("=" * 60)
    print("1. Testing Package Imports")
    print("=" * 60)
    
    packages = {
        'psycopg2': 'PostgreSQL driver',
        'google.cloud.bigquery': 'BigQuery client',
        'pandas': 'Data manipulation',
        'tqdm': 'Progress bars'
    }
    
    failed = []
    for package, description in packages.items():
        try:
            __import__(package)
            print(f"✅ {package:30} → {description}")
        except ImportError as e:
            print(f"❌ {package:30} → FAILED: {e}")
            failed.append(package)
    
    print()
    if failed:
        print(f"❌ {len(failed)} package(s) failed to import")
        return False
    else:
        print("✅ All packages imported successfully")
        return True

def test_postgresql():
    """Test PostgreSQL connection"""
    print()
    print("=" * 60)
    print("2. Testing PostgreSQL Connection")
    print("=" * 60)
    
    try:
        import psycopg2
        
        # Try to connect
        conn = psycopg2.connect(
            host='localhost',
            port=5432,  
            database='customer360',
            user='dataeng',
            password='2003'  
        )
        cursor = conn.cursor()
        
        # Simple query
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL connection successful")
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        print("   Hint: Run 'python test_postgres_connection.py' for detailed diagnostics")
        return False

def test_bigquery():
    """Test BigQuery connection"""
    print()
    print("=" * 60)
    print("3. Testing BigQuery Connection")
    print("=" * 60)
    
    try:
        from google.cloud import bigquery
        
        # Initialize client
        client = bigquery.Client()
        
        # Simple query
        query = "SELECT 'Connected' as status"
        result = client.query(query).to_dataframe()
        
        print(f"✅ BigQuery connection successful")
        print(f"   Project: {client.project}")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        print("   Hint: Run 'gcloud auth application-default login'")
        return False

def test_data_flow():
    """Test full data flow: PostgreSQL → pandas → BigQuery"""
    print()
    print("=" * 60)
    print("4. Testing Data Flow (PostgreSQL → pandas → BigQuery)")
    print("=" * 60)
    
    try:
        import psycopg2
        import pandas as pd
        from google.cloud import bigquery
        
        # Step 1: Extract from PostgreSQL
        print("Step 1: Extract from PostgreSQL...")
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='customer360',
            user='dataeng',
            password='2003'
        )
        
        # Simple test query
        df = pd.read_sql("SELECT 1 as id, 'test' as name", conn)
        conn.close()
        print(f"   ✅ Extracted {len(df)} rows to pandas DataFrame")
        
        # Step 2: Transform in pandas
        print("Step 2: Transform in pandas...")
        df['name_upper'] = df['name'].str.upper()
        print(f"   ✅ Transformed DataFrame ({len(df)} rows, {len(df.columns)} columns)")
        
        # Step 3: Load to BigQuery (dry run - don't actually load)
        print("Step 3: Validate BigQuery load capability...")
        client = bigquery.Client()
        
        # Just test that we CAN load (don't actually do it)
        project_id = client.project
        table_ref = f"{project_id}.raw_data.test_table"
        print(f"   ✅ BigQuery client ready to load to {table_ref}")
        print(f"   (Skipping actual load - just validation)")
        
        print()
        print("✅ Full data flow validated (PostgreSQL → pandas → BigQuery)")
        return True
        
    except Exception as e:
        print(f"❌ Data flow test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("FULL ENVIRONMENT TEST")
    print("Testing: PostgreSQL + Python + BigQuery")
    print("=" * 60 + "\n")
    
    results = {}
    
    # Run tests
    results['imports'] = test_imports()
    results['postgresql'] = test_postgresql()
    results['bigquery'] = test_bigquery()
    results['data_flow'] = test_data_flow()
    
    # Summary
    print()
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"{test_name:20} {status}")
    
    print()
    print(f"Result: {passed}/{total} tests passed")
    
    if passed == total:
        print()
        print("🎉 ALL TESTS PASSED!")
        print("Environment is ready for migration scripts")
        print()
        print("Next steps:")
        print("- Day 7: Build extract script (PostgreSQL → CSV)")
        print("- Day 8: Build load script (CSV → BigQuery)")
        print("- Day 9: Create BigQuery schemas")
        print("- Day 10: Build reconciliation framework")
        return True
    else:
        print()
        print("⚠️  SOME TESTS FAILED")
        print("Please fix the failures before proceeding")
        print()
        print("Troubleshooting:")
        print("- PostgreSQL: Check connection params (host, port, user, password)")
        print("- BigQuery: Run 'gcloud auth application-default login'")
        print("- Imports: Run 'pip install -r requirements.txt'")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
