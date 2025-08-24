from sqlalchemy import create_engine, text
#from psycopg2-binary

# Connection string to the default "postgres" database
engine = create_engine("postgresql://postgres:1@localhost:5432/postgres", echo=True)

# Connect and create a new database
with engine.connect() as conn:
    conn.execute(text("COMMIT"))  # required outside transaction
    conn.execute(text("CREATE DATABASE real_estate"))

print("✅ Database 'real_estate' created successfully!")