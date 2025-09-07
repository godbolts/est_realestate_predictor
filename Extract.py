import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Float, Integer
import json

# Connection string to the default "postgres" database (superuser)
admin_engine = create_engine("postgresql://postgres:1@localhost:5432/postgres", echo=True)

db_name = "kaggle_test"
db_user = "kaggle_user"
db_password = "TEMPORARYPASSWORD"

# Step 1: Ensure DB exists (using postgres superuser)
with admin_engine.connect() as conn:
    conn.execute(text("COMMIT"))
    db_exists = conn.execute(
        text("SELECT 1 FROM pg_database WHERE datname = :name"),
        {"name": db_name}
    ).fetchone()

    if not db_exists:
        conn.execute(text(f"CREATE DATABASE {db_name}"))
        print(f"✅ Database '{db_name}' created.")
    else:
        print(f"ℹ️ Database '{db_name}' already exists.")

# Step 2: Ensure user exists
with admin_engine.connect() as conn:
    conn.execute(text("COMMIT"))
    user_exists = conn.execute(
        text("SELECT 1 FROM pg_roles WHERE rolname = :name"),
        {"name": db_user}
    ).fetchone()

    if not user_exists:
        conn.execute(text(f"CREATE USER {db_user} WITH PASSWORD '{db_password}'"))
        print(f"✅ User '{db_user}' created.")
    else:
        print(f"ℹ️ User '{db_user}' already exists.")

# Step 3: Connect to the target DB as superuser to grant schema rights
with create_engine(f"postgresql://postgres:1@localhost:5432/{db_name}").connect() as conn:
    conn.execute(text("COMMIT"))
    conn.execute(text(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user}"))
    conn.execute(text(f"GRANT ALL PRIVILEGES ON SCHEMA public TO {db_user}"))
    conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO {db_user}"))
    conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO {db_user}"))
    print(f"✅ Privileges granted for '{db_user}' in database '{db_name}'.")

# Step 4: Connect as the new user
user_engine = create_engine(
    f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}",
    echo=True
)

api_root = "https://api.worldbank.org/v2/country/all/indicator"
debt = "GC.DOD.TOTL.GD.ZS"

url = f"{api_root}/{debt}?format=json&per_page=1000"

print(f"📡 Fetching: {url}")
response = requests.get(url)

#if response.status_code != 200:
#    print(f"❌ Error {response.status_code}: {response.text}")
#else:
#    data = response.json()
    # World Bank API returns a list: [metadata, actual data]
#    metadata, records = data[0], data[1]

#    print("ℹ️ Metadata:")
#    print(metadata)

#    print("\n📊 Sample records:")
#    for row in records[:5]:  # show first 5 records
#        print(row)

    # Optional: put into a DataFrame for easier inspection
#    df = pd.DataFrame(records)
#    print("\nDataFrame head:")
#    print(df.head())


api_root = "https://api.worldbank.org/v2/country/all/indicator"
debt = "GC.DOD.TOTL.GD.ZS"

all_records = []
page = 1

while True:
    url = f"{api_root}/{debt}?format=json&per_page=1000&page={page}"
    resp = requests.get(url)

    if resp.status_code != 200:
        print(f"❌ Error {resp.status_code} on page {page}")
        print(resp.text[:500])  # show first 500 chars for debugging
        break

    try:
        data = resp.json()
    except Exception:
        print(f"❌ Could not parse JSON on page {page}")
        print(resp.text[:500])  # in case it's HTML error
        break

    meta, records = data[0], data[1]
    all_records.extend(records)

    print(f"📄 Page {page}/{meta['pages']} fetched ({len(records)} rows).")

    if page >= meta['pages']:
        break
    page += 1

df = pd.json_normalize(all_records)
print(f"✅ Got {len(df)} rows total")

# Rename nested column names so they’re easier in SQL
df.rename(columns={
    "indicator.id": "indicator_id",
    "indicator.value": "indicator_name",
    "country.id": "country_id",
    "country.value": "country_name"
}, inplace=True)

# Save DataFrame into Postgres
df.to_sql(
    "worldbank_debt",          # table name
    user_engine,               # your SQLAlchemy engine
    if_exists="replace",       # replace if table already exists
    index=False,               # don’t save DataFrame index
    dtype={
        "indicator_id": String,
        "indicator_name": String,
        "country_id": String(3),
        "country_name": String,
        "countryiso3code": String(3),
        "date": Integer,
        "value": Float,
        "unit": String,
        "obs_status": String,
        "decimal": Integer
    }
)

print("📥 Data saved into table 'worldbank_debt'")

# ---------------------------
# DIMENSION TABLE: indicators
# ---------------------------
indicators_url = "https://api.worldbank.org/v2/indicator?format=json&per_page=20000"
indicators = requests.get(indicators_url).json()[1]
df_indicators = pd.json_normalize(indicators)

# Convert topics (list of dicts) to JSON strings
if "topics" in df_indicators.columns:
    df_indicators["topics"] = df_indicators["topics"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

df_indicators.to_sql(
    "worldbank_indicators",
    user_engine,
    if_exists="replace",
    index=False,
    dtype={col: String for col in df_indicators.columns}
)
print("📥 Data saved into table 'worldbank_indicators'")

# ---------------------------
# DIMENSION TABLE: countries
# ---------------------------
countries_url = "https://api.worldbank.org/v2/country?format=json&per_page=400"
countries = requests.get(countries_url).json()[1]
df_countries = pd.json_normalize(countries)

df_countries.to_sql(
    "worldbank_countries",
    user_engine,
    if_exists="replace",
    index=False,
    dtype={col: String for col in df_countries.columns}
)
print("📥 Data saved into table 'worldbank_countries'")