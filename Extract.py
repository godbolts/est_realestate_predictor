import pandas as pd
import eurostat  # pip install eurostat
from sqlalchemy import create_engine, text

# Connection string to the default "postgres" database (superuser)
admin_engine = create_engine("postgresql://postgres:1@localhost:5432/postgres", echo=True)

db_name = "real_estate"
db_user = "real_estate_user"
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