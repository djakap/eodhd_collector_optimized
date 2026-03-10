# Power BI Connection Guide for EODHD QuestDB

## Issue: PostgreSQL Connector Doesn't Work
Power BI's native PostgreSQL connector queries system tables (pg_enum, pg_catalog) that don't exist in QuestDB.

## ✅ Solution 1: Use Web Connector (Recommended)

### Connection Details:
- **URL**: `http://localhost:9000/exp`
- **Query Parameter**: Add SQL via `?query=`
- **Format**: CSV (automatic)

### Step-by-Step:

1. **Get Data** → **Web**
2. **Advanced** → URL parts:
   - **URL**: `http://localhost:9000/exp`
   - **Query**: Add parameter `query` with your SQL

### Sample Queries:

#### 1. Daily Price Data (Last 1000 records)
```
http://localhost:9000/exp?query=SELECT symbol, timestamp, open, high, low, close, volume FROM eodhd_stock_data WHERE interval = 'd' ORDER BY timestamp DESC LIMIT 1000
```

#### 2. Specific Stocks - Last 6 Months
```
http://localhost:9000/exp?query=SELECT * FROM eodhd_stock_data WHERE interval = 'd' AND symbol IN ('BBCA.JK', 'BBRI.JK', 'BMRI.JK') AND timestamp > dateadd('M', -6, now()) ORDER BY timestamp DESC
```

#### 3. Corporate Actions
```
http://localhost:9000/exp?query=SELECT symbol, action_type, action_date, dividend_amount, split_ratio FROM eodhd_corporate_actions ORDER BY action_date DESC
```

#### 4. Stock Metadata
```
http://localhost:9000/exp?query=SELECT symbol, name, exchange, sector, industry, market_cap FROM eodhd_metadata
```

#### 5. Intraday Data (5-minute)
```
http://localhost:9000/exp?query=SELECT symbol, timestamp, close, volume FROM eodhd_stock_data WHERE interval = '5m' AND symbol = 'BBCA.JK' AND timestamp > dateadd('d', -7, now()) ORDER BY timestamp DESC
```

---

## ✅ Solution 2: Use ODBC with Custom Connection String

### Install PostgreSQL ODBC Driver:
```bash
# Ubuntu/Debian
sudo apt-get install odbc-postgresql

# Windows - Download from:
# https://www.postgresql.org/ftp/odbc/versions/msi/
```

### In Power BI - Advanced Editor (Blank Query):

```m
let
    ConnectionString = "Driver={PostgreSQL Unicode};Server=localhost;Port=8812;Database=qdb;UID=admin;PWD=quest;UseServerSidePrepare=0;Protocol=7.4",
    
    Source = Odbc.Query(ConnectionString, "SELECT * FROM eodhd_stock_data WHERE interval = 'd' LIMIT 10000")
in
    Source
```

**Important ODBC Parameters:**
- `UseServerSidePrepare=0` - Prevents some incompatibility issues
- `Protocol=7.4` - Uses older PostgreSQL protocol version

---

## ✅ Solution 3: Python/REST Bridge (Most Reliable)

Create a Python script that exposes data via Flask API, then connect Power BI to that.

### Create REST API Server:

```python
# powerbi_api.py
from flask import Flask, jsonify, request
from db.questdb_client import QuestDBClient
import pandas as pd

app = Flask(__name__)

@app.route('/api/daily_prices')
def get_daily_prices():
    """Get daily price data"""
    symbol = request.args.get('symbol', None)
    limit = int(request.args.get('limit', 1000))
    
    client = QuestDBClient()
    client.connect()
    
    query = f"""
    SELECT symbol, timestamp, open, high, low, close, volume 
    FROM eodhd_stock_data 
    WHERE interval = 'd'
    {f"AND symbol = '{symbol}'" if symbol else ""}
    ORDER BY timestamp DESC 
    LIMIT {limit}
    """
    
    client.cursor.execute(query)
    data = client.cursor.fetchall()
    client.close()
    
    # Convert to dict
    columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    result = [dict(zip(columns, row)) for row in data]
    
    return jsonify(result)

@app.route('/api/corporate_actions')
def get_corporate_actions():
    """Get corporate actions"""
    client = QuestDBClient()
    client.connect()
    
    query = """
    SELECT symbol, action_type, action_date, dividend_amount, split_ratio
    FROM eodhd_corporate_actions
    ORDER BY action_date DESC
    """
    
    client.cursor.execute(query)
    data = client.cursor.fetchall()
    client.close()
    
    columns = ['symbol', 'action_type', 'action_date', 'dividend_amount', 'split_ratio']
    result = [dict(zip(columns, row)) for row in data]
    
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
```

**Run the API:**
```bash
pip install flask
python powerbi_api.py
```

**In Power BI:**
- **Get Data** → **Web** → `http://localhost:5000/api/daily_prices?limit=10000`

---

## ✅ Solution 4: Export to CSV/Parquet (For Static Analysis)

Create export script:

```python
# export_for_powerbi.py
import sys
sys.path.insert(0, '/home/djp/eodhd_collector_optimized')

from db.questdb_client import QuestDBClient
import pandas as pd

client = QuestDBClient()
client.connect()

# Export daily prices
print("Exporting daily prices...")
client.cursor.execute("""
    SELECT symbol, timestamp, open, high, low, close, volume 
    FROM eodhd_stock_data 
    WHERE interval = 'd'
    ORDER BY timestamp DESC
""")
df_prices = pd.DataFrame(client.cursor.fetchall(), 
                         columns=['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume'])
df_prices.to_parquet('powerbi_data/daily_prices.parquet')
df_prices.to_csv('powerbi_data/daily_prices.csv', index=False)

# Export corporate actions
print("Exporting corporate actions...")
client.cursor.execute("""
    SELECT symbol, action_type, action_date, dividend_amount, split_ratio
    FROM eodhd_corporate_actions
""")
df_actions = pd.DataFrame(client.cursor.fetchall(),
                          columns=['symbol', 'action_type', 'action_date', 'dividend_amount', 'split_ratio'])
df_actions.to_parquet('powerbi_data/corporate_actions.parquet')
df_actions.to_csv('powerbi_data/corporate_actions.csv', index=False)

# Export metadata
print("Exporting metadata...")
client.cursor.execute("SELECT * FROM eodhd_metadata")
df_metadata = pd.DataFrame(client.cursor.fetchall())
df_metadata.to_parquet('powerbi_data/metadata.parquet')
df_metadata.to_csv('powerbi_data/metadata.csv', index=False)

client.close()
print("✅ Export complete! Files in powerbi_data/")
```

Then in Power BI: **Get Data** → **Folder** → Select `powerbi_data/`

---

## 📊 Recommended Approach

**For your 8M+ records:**

1. **For Dashboards**: Use **Web Connector** with filtered queries (most recent data)
2. **For Ad-hoc Analysis**: Use **Python REST API** (most flexible)
3. **For Static Reports**: Use **Parquet export** (fastest loading)

## Sample Power BI M Code (Web Connector)

```m
let
    BaseUrl = "http://localhost:9000/exp?query=",
    
    SqlQuery = "SELECT symbol, timestamp as Date, open as Open, high as High, low as Low, close as Close, volume as Volume FROM eodhd_stock_data WHERE interval = 'd' AND timestamp > dateadd('M', -12, now()) ORDER BY timestamp DESC",
    
    EncodedQuery = Uri.EscapeDataString(SqlQuery),
    
    FullUrl = BaseUrl & EncodedQuery,
    
    Source = Csv.Document(Web.Contents(FullUrl),[Delimiter=",", Columns=7, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"symbol", type text}, {"Date", type datetime}, {"Open", type number}, {"High", type number}, {"Low", type number}, {"Close", type number}, {"Volume", Int64.Type}})
in
    #"Changed Type"
```

---

## Need Help?

Check QuestDB is running:
```bash
curl "http://localhost:9000/exec?query=SELECT COUNT(*) FROM eodhd_stock_data"
```

Should return JSON with count.
