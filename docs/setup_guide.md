
---

## Python Environment Setup (Day 6)

### Installed Packages
```bash
# Core dependencies
google-api-core==2.29.0
google-auth==2.47.0
google-cloud-bigquery==3.40.0
google-cloud-bigquery-storage==2.36.0
google-cloud-core==2.5.0
google-crc32c==1.8.0
google-resumable-media==2.8.0
googleapis-common-protos==1.72.0
pandas==2.3.3
psycopg2-binary==2.9.11
tqdm==4.67.1
```

### Installation
```bash
# Activate virtual environment
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt

# Verify installation
python test_full_environment.py
```

### Connection Configuration

#### PostgreSQL
```python
conn = psycopg2.connect(
    host='localhost',
    port=5432,  
    database='customer360',
    user='dataeng,
    password='2003
)
```

#### BigQuery
```python
from google.cloud import bigquery

# Uses application default credentials
# Run: gcloud auth application-default login
client = bigquery.Client()
```

### Troubleshooting

**PostgreSQL connection failed:**
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Or for Docker
docker ps | grep postgres

# Test connection
python test_postgres_connection.py
```

**BigQuery auth failed:**
```bash
# Re-authenticate
gcloud auth application-default login

# Verify project
gcloud config get-value project

# Test connection
python test_bigquery.py
```

**Import errors:**
```bash
# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Verify
pip list | grep -E "(psycopg|google|pandas)"
```

