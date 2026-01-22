"""
Test PostgreSQL connection
Checks if we can connect to customer360 database
"""

import psycopg2
import sys

def test_postgres_connection():
    """Test connection to PostgreSQL database"""
    
    # Connection parameters - ADJUST THESE!
    conn_params = {
        'host': 'localhost',
        'port': 5432,  
        'database': 'customer360',
        'user': 'dataeng',  
        'password': '2003'  
    }
    
    print("=" * 60)
    print("Testing PostgreSQL Connection")
    print("=" * 60)
    print(f"Host: {conn_params['host']}:{conn_params['port']}")
    print(f"Database: {conn_params['database']}")
    print(f"User: {conn_params['user']}")
    print()
    
    try:
        # Attempt connection
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Test query
        print("✅ Connection successful!")
        print()
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"PostgreSQL version: {version[:50]}...")
        print()
        
        # List databases
        cursor.execute("""
            SELECT datname 
            FROM pg_database 
            WHERE datistemplate = false
            ORDER BY datname;
        """)
        databases = cursor.fetchall()
        print(f"Available databases: {[db[0] for db in databases]}")
        print()
        
        # Check if customer360 exists
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 
                FROM pg_database 
                WHERE datname = 'customer360'
            );
        """)
        exists = cursor.fetchone()[0]
        
        if exists:
            print("✅ Database 'customer360' exists!")
            
            # Check schemas
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata
                WHERE schema_name IN ('raw', 'staging', 'warehouse')
                ORDER BY schema_name;
            """)
            schemas = cursor.fetchall()
            print(f"   Schemas found: {[s[0] for s in schemas]}")
            
            # Check tables in raw schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            if tables:
                print(f"   Tables in raw: {[t[0] for t in tables]}")
            else:
                print("   ⚠️  No tables found in 'raw' schema")
        else:
            print("⚠️  Database 'customer360' does NOT exist")
            print("   You may need to create it or use a different database name")
        
        # Close connection
        cursor.close()
        conn.close()
        
        print()
        print("=" * 60)
        print("✅ PostgreSQL connection test PASSED")
        print("=" * 60)
        return True
        
    except psycopg2.OperationalError as e:
        print()
        print("=" * 60)
        print("❌ Connection FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        print("Possible solutions:")
        print("1. Check if PostgreSQL is running:")
        print("   sudo systemctl status postgresql")
        print("   OR: docker ps | grep postgres")
        print()
        print("2. Verify connection parameters:")
        print(f"   Host: {conn_params['host']}")
        print(f"   Port: {conn_params['port']}")
        print(f"   Database: {conn_params['database']}")
        print(f"   User: {conn_params['user']}")
        print()
        print("3. If using Docker container on port 5433:")
        print("   Change conn_params['port'] = 5433")
        print()
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_postgres_connection()
    sys.exit(0 if success else 1)
