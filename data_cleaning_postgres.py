"""
Luxury Housing Sales Analysis - Bengaluru
Full Pipeline: Data Cleaning + Feature Engineering + PostgreSQL Load
Author: Analytics Pipeline
Requirements: pip install pandas numpy sqlalchemy psycopg2-binary python-dotenv
"""

import pandas as pd
import numpy as np
import re
import os
import warnings
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION — update these values or use a .env file
# ─────────────────────────────────────────────────────────────────
# Option A: Hardcode credentials (for local dev only)
# DB_CONFIG = {
#     "host":     "localhost",       # e.g. "localhost" or "your-rds-host.amazonaws.com"
#     "port":     5432,              # Default PostgreSQL port
#     "database": "luxury_housing_db",
#     "username": "postgres",        # Your PostgreSQL username
#     "password": "your_password",   # Your PostgreSQL password
# }

# Option B: Load from .env file (recommended for production)
# Create a .env file with:
#   PG_HOST=localhost
#   PG_PORT=5432
#   PG_DB=luxury_housing_db
#   PG_USER=postgres
#   PG_PASSWORD=your_password
# Then uncomment the block below:
#
load_dotenv()
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "luxury_housing_db"),
    "username": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

TABLE_NAME  = "luxury_housing"
SCHEMA_NAME = "public"           # Change if using a custom schema


# ─────────────────────────────────────────────────────────────────
# 1. DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────

def get_engine():
    """
    Build SQLAlchemy engine for PostgreSQL.
    Connection string format:
        postgresql+psycopg2://username:password@host:port/database
    """
    url = (
        f"postgresql+psycopg2://"
        f"{DB_CONFIG['username']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f"/{DB_CONFIG['database']}"
    )
    engine = create_engine(
        url,
        pool_pre_ping=True,      # checks connection is alive before using it
        pool_size=5,
        max_overflow=10,
        echo=False               # set True to log all SQL statements
    )
    return engine


def test_connection(engine) -> bool:
    """Verify the database connection is working."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[INFO] PostgreSQL connection successful.")
        return True
    except SQLAlchemyError as e:
        print(f"[ERROR] Connection failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────
# 2. LOAD DATA
# ─────────────────────────────────────────────────────────────────

def load_data(filepath: str) -> pd.DataFrame:
    """Load raw CSV dataset."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV not found: {filepath}")
    df = pd.read_csv(filepath)
    print(f"[INFO] Loaded {len(df):,} rows × {df.shape[1]} columns")
    return df


# ─────────────────────────────────────────────────────────────────
# 3. CLEAN TICKET PRICE
# ─────────────────────────────────────────────────────────────────

def clean_ticket_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Ticket_Price_Cr — handles mixed formats:
      '₹9.82 Cr'  →  9.82
      '12.750846' →  12.75
      NaN         →  median of that Micro_Market
    """
    def parse_price(val):
        if pd.isna(val):
            return np.nan
        val = str(val).strip()
        val = re.sub(r'[₹,]', '', val)
        val = re.sub(r'(?i)\s*cr\s*', '', val).strip()
        try:
            return float(val)
        except ValueError:
            return np.nan

    df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].apply(parse_price)
    median_price = df.groupby('Micro_Market')['Ticket_Price_Cr'].transform('median')
    df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].fillna(median_price).round(4)
    print(f"[INFO] Ticket_Price_Cr — nulls remaining: {df['Ticket_Price_Cr'].isna().sum()}")
    return df


# ─────────────────────────────────────────────────────────────────
# 4. NORMALIZE TEXT FIELDS
# ─────────────────────────────────────────────────────────────────

def normalize_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize casing and whitespace across all text columns."""

    # Micro_Market → Title Case  (e.g. WHITEFIELD → Whitefield)
    df['Micro_Market'] = df['Micro_Market'].str.strip().str.title()

    # Configuration → UPPERCASE canonical forms (3BHK, 4BHK, 5BHK+)
    def normalize_config(val):
        if pd.isna(val):
            return val
        val = str(val).strip().upper().replace(' ', '')
        val = re.sub(r'5BHK\+?', '5BHK+', val)
        return val
    df['Configuration'] = df['Configuration'].apply(normalize_config)

    # Other categorical text columns → Title Case
    for col in ['Developer_Name', 'Sales_Channel', 'Buyer_Type',
                'Possession_Status', 'Transaction_Type']:
        df[col] = df[col].astype(str).str.strip().str.title()

    print("[INFO] Text fields normalized.")
    return df


# ─────────────────────────────────────────────────────────────────
# 5. HANDLE NULLS
# ─────────────────────────────────────────────────────────────────

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputation strategy:
      Unit_Size_Sqft  → median per Configuration group
      Amenity_Score   → median per Micro_Market group
      Buyer_Comments  → fill with 'No Comment'
    """
    # Unit_Size_Sqft
    median_size = df.groupby('Configuration')['Unit_Size_Sqft'].transform('median')
    df['Unit_Size_Sqft'] = df['Unit_Size_Sqft'].fillna(median_size).round(0)

    # Amenity_Score
    median_amenity = df.groupby('Micro_Market')['Amenity_Score'].transform('median')
    df['Amenity_Score'] = df['Amenity_Score'].fillna(median_amenity).round(4)

    # Buyer_Comments
    df['Buyer_Comments'] = df['Buyer_Comments'].fillna('No Comment')

    remaining = df.isnull().sum()
    remaining = remaining[remaining > 0]
    if remaining.empty:
        print("[INFO] All nulls handled — dataset is complete.")
    else:
        print(f"[WARN] Remaining nulls:\n{remaining}")
    return df


# ─────────────────────────────────────────────────────────────────
# 6. FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive analytical columns required by the Power BI dashboard."""

    # Price per Sqft in Lakhs
    df['Price_Per_Sqft_L'] = (
        (df['Ticket_Price_Cr'] * 100) / df['Unit_Size_Sqft']
    ).round(4)

    # Date-based features from Purchase_Quarter
    df['Purchase_Quarter'] = pd.to_datetime(df['Purchase_Quarter'], errors='coerce')
    df['Year']             = df['Purchase_Quarter'].dt.year.astype('Int64')
    df['Quarter_Number']   = df['Purchase_Quarter'].dt.quarter.astype('Int64')
    df['Quarter_Label']    = 'Q' + df['Quarter_Number'].astype(str) + '-' + df['Year'].astype(str)

    # Convert Purchase_Quarter back to string for PostgreSQL DATE compatibility
    df['Purchase_Quarter'] = df['Purchase_Quarter'].dt.strftime('%Y-%m-%d')

    # Booking_Flag: 1 = Primary booking, 0 = Secondary
    df['Booking_Flag'] = (
        df['Transaction_Type'].str.lower() == 'primary'
    ).astype(int)

    # NRI_Buyer_Flag: 1 = NRI buyer, 0 = Resident
    df['NRI_Buyer_Flag'] = (
        df['NRI_Buyer'].str.lower() == 'yes'
    ).astype(int)

    # Amenity Tier classification
    def amenity_tier(score):
        if score >= 8:  return 'Premium'
        elif score >= 5: return 'Standard'
        else:            return 'Basic'
    df['Amenity_Tier'] = df['Amenity_Score'].apply(amenity_tier)

    # Price Segment classification
    def price_segment(price):
        if price >= 15:  return 'Ultra Luxury (15Cr+)'
        elif price >= 10: return 'Luxury (10-15Cr)'
        elif price >= 5:  return 'Premium (5-10Cr)'
        else:             return 'Affordable (<5Cr)'
    df['Price_Segment'] = df['Ticket_Price_Cr'].apply(price_segment)

    print("[INFO] Feature engineering complete — new columns added:")
    print("       Price_Per_Sqft_L, Year, Quarter_Number, Quarter_Label,")
    print("       Booking_Flag, NRI_Buyer_Flag, Amenity_Tier, Price_Segment")
    return df


# ─────────────────────────────────────────────────────────────────
# 7. RENAME COLUMNS TO MATCH PROJECT SPECIFICATION
# ─────────────────────────────────────────────────────────────────

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Align column names with the project PDF specification."""
    df = df.rename(columns={
        'Property_ID':      'Project_ID',
        'Developer_Name':   'Builder',
        'Transaction_Type': 'Booking_Status',
    })
    return df


# ─────────────────────────────────────────────────────────────────
# 8. LOAD TO POSTGRESQL
# ─────────────────────────────────────────────────────────────────

def load_to_postgres(df: pd.DataFrame, engine) -> None:
    """
    Insert cleaned DataFrame into PostgreSQL table.

    Uses pandas to_sql with chunksize for large datasets.
    if_exists='replace' drops and recreates the table each run.
    Change to if_exists='append' if you want to add rows instead.
    """
    print(f"\n[INFO] Loading {len(df):,} rows to PostgreSQL → {SCHEMA_NAME}.{TABLE_NAME} ...")

    try:
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists='replace',   # 'replace' = drop + recreate | 'append' = add rows
            index=False,
            chunksize=5000,        # insert 5,000 rows at a time (avoids memory issues)
            method='multi'         # faster bulk insert
        )
        print(f"[INFO] Successfully loaded to PostgreSQL table: {SCHEMA_NAME}.{TABLE_NAME}")
    except SQLAlchemyError as e:
        print(f"[ERROR] Failed to load data to PostgreSQL: {e}")
        raise


def add_indexes(engine) -> None:
    """
    Add indexes on high-usage columns for faster Power BI queries.
    Runs after the table is created by to_sql.
    """
    indexes = [
        f'CREATE INDEX IF NOT EXISTS idx_builder       ON {SCHEMA_NAME}.{TABLE_NAME} ("Builder");',
        f'CREATE INDEX IF NOT EXISTS idx_micro_market  ON {SCHEMA_NAME}.{TABLE_NAME} ("Micro_Market");',
        f'CREATE INDEX IF NOT EXISTS idx_quarter_label ON {SCHEMA_NAME}.{TABLE_NAME} ("Quarter_Label");',
        f'CREATE INDEX IF NOT EXISTS idx_booking_flag  ON {SCHEMA_NAME}.{TABLE_NAME} ("Booking_Flag");',
        f'CREATE INDEX IF NOT EXISTS idx_configuration ON {SCHEMA_NAME}.{TABLE_NAME} ("Configuration");',
        f'CREATE INDEX IF NOT EXISTS idx_sales_channel ON {SCHEMA_NAME}.{TABLE_NAME} ("Sales_Channel");',
    ]
    try:
        with engine.connect() as conn:
            for sql in indexes:
                conn.execute(text(sql))
            conn.commit()
        print("[INFO] Indexes created successfully.")
    except SQLAlchemyError as e:
        print(f"[WARN] Index creation skipped (non-critical): {e}")


# ─────────────────────────────────────────────────────────────────
# 9. VALIDATION QUERIES
# ─────────────────────────────────────────────────────────────────

def run_validation_queries(engine) -> None:
    """Run post-load SQL checks and print results."""
    queries = {
        "Total rows loaded": f'SELECT COUNT(*) FROM {SCHEMA_NAME}."{TABLE_NAME}"',

        "Rows by Booking_Status": f'''
            SELECT "Booking_Status", COUNT(*) AS count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM {SCHEMA_NAME}."{TABLE_NAME}"
            GROUP BY "Booking_Status"
            ORDER BY count DESC
        ''',

        "Avg ticket price per Builder": f'''
            SELECT "Builder",
                   ROUND(AVG("Ticket_Price_Cr")::numeric, 3) AS avg_price_cr,
                   ROUND(SUM("Ticket_Price_Cr")::numeric, 2) AS total_revenue_cr,
                   COUNT(*) AS units
            FROM {SCHEMA_NAME}."{TABLE_NAME}"
            GROUP BY "Builder"
            ORDER BY total_revenue_cr DESC
        ''',

        "Null check on key columns": f'''
            SELECT
                SUM(CASE WHEN "Ticket_Price_Cr" IS NULL THEN 1 ELSE 0 END) AS null_price,
                SUM(CASE WHEN "Amenity_Score"   IS NULL THEN 1 ELSE 0 END) AS null_amenity,
                SUM(CASE WHEN "Unit_Size_Sqft"  IS NULL THEN 1 ELSE 0 END) AS null_size,
                SUM(CASE WHEN "Buyer_Comments"  IS NULL THEN 1 ELSE 0 END) AS null_comments
            FROM {SCHEMA_NAME}."{TABLE_NAME}"
        ''',

        "Booking conversion rate by Micro_Market": f'''
            SELECT "Micro_Market",
                   COUNT(*) AS total_units,
                   SUM("Booking_Flag") AS primary_bookings,
                   ROUND(SUM("Booking_Flag") * 100.0 / COUNT(*), 2) AS conversion_pct
            FROM {SCHEMA_NAME}."{TABLE_NAME}"
            GROUP BY "Micro_Market"
            ORDER BY conversion_pct DESC
            LIMIT 5
        ''',
    }

    print("\n" + "=" * 60)
    print("VALIDATION QUERY RESULTS")
    print("=" * 60)
    try:
        with engine.connect() as conn:
            for title, sql in queries.items():
                print(f"\n--- {title} ---")
                result = pd.read_sql(text(sql), conn)
                print(result.to_string(index=False))
    except SQLAlchemyError as e:
        print(f"[ERROR] Validation failed: {e}")


# ─────────────────────────────────────────────────────────────────
# 10. FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("FINAL CLEANED DATASET SUMMARY")
    print("=" * 60)
    print(f"  Rows          : {len(df):,}")
    print(f"  Columns       : {df.shape[1]}")
    print(f"  Null values   : {df.isnull().sum().sum()}")
    print(f"  Builders      : {df['Builder'].nunique()} → {sorted(df['Builder'].unique().tolist())}")
    print(f"  Micro-Markets : {df['Micro_Market'].nunique()} → {sorted(df['Micro_Market'].unique().tolist())}")
    print(f"  Configurations: {sorted(df['Configuration'].unique().tolist())}")
    print(f"  Booking Status: {df['Booking_Status'].unique().tolist()}")
    print(f"  Quarter Labels: {sorted(df['Quarter_Label'].unique().tolist())}")
    print(f"  Year Range    : {df['Year'].min()} – {df['Year'].max()}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────

def run_pipeline(input_path: str, output_csv: str) -> pd.DataFrame:
    """Execute the full cleaning + load pipeline."""

    # Step 1: Load
    df = load_data(input_path)

    # Step 2: Clean price
    df = clean_ticket_price(df)

    # Step 3: Normalize text
    df = normalize_text_fields(df)

    # Step 4: Handle nulls
    df = handle_nulls(df)

    # Step 5: Feature engineering
    df = engineer_features(df)

    # Step 6: Rename columns
    df = rename_columns(df)

    # Step 7: Print summary
    print_summary(df)

    # Step 8: Save cleaned CSV (backup)
    df.to_csv(output_csv, index=False)
    print(f"\n[INFO] Cleaned CSV saved: {output_csv}")

    # Step 9: Connect to PostgreSQL
    engine = get_engine()
    if not test_connection(engine):
        print("[ERROR] Aborting — could not connect to PostgreSQL.")
        return df

    # Step 10: Load to PostgreSQL
    load_to_postgres(df, engine)

    # Step 11: Add indexes
    add_indexes(engine)

    # Step 12: Validate
    run_validation_queries(engine)

    engine.dispose()
    print("\n[DONE] Full pipeline complete. Database is ready for Power BI.")
    return df


# ─────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
    INPUT_FILE  = os.path.join(BASE_DIR, 'Luxury_Housing_Bangalore.csv')
    OUTPUT_CSV  = os.path.join(BASE_DIR, 'luxury_housing_cleaned.csv')

    df_clean = run_pipeline(INPUT_FILE, OUTPUT_CSV)
