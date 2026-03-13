# 🏙️ Luxury Housing Sales Analysis — Bengaluru

> **GUVI × HCL Capstone Project** | Real Estate Analytics Pipeline
> Python · PostgreSQL · Power BI · ETL · Feature Engineering

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Dataset Description](#dataset-description)
- [Tech Stack](#tech-stack)
- [Setup & Installation](#setup--installation)
- [Step 1 — Python: Data Cleaning & Feature Engineering](#step-1--python-data-cleaning--feature-engineering)
- [Step 2 — PostgreSQL: Load & Validate](#step-2--postgresql-load--validate)
- [Step 3 — Power BI: Dashboard & Visualizations](#step-3--power-bi-dashboard--visualizations)
- [Engineered Features](#engineered-features)
- [Power BI Visualizations (10 Questions)](#power-bi-visualizations-10-questions)
- [DAX Measures](#dax-measures)
- [Key Business Insights](#key-business-insights)
- [Deliverables Checklist](#deliverables-checklist)

---

## Project Overview

This project builds a **complete end-to-end real estate analytics pipeline** for the Bengaluru luxury housing market using 101,000+ transaction records.

The pipeline covers three stages:

```
Raw CSV (101,000 rows)
        │
        ▼
 Python Cleaning & Feature Engineering
        │
        ▼
 PostgreSQL Database (luxury_housing table)
        │
        ▼
 Power BI Dashboard (10 Visualizations)
```

**Business goals addressed:**
- Identify high-performing localities and builders
- Track quarterly sales trends across micro-markets
- Analyze amenity scores vs. booking conversion rates
- Understand buyer personas (HNI, NRI, CXO, Startup Founders)
- Benchmark pricing strategies across configurations

---

## Project Structure

```
luxury-housing-analysis/
│
├── data/
│   ├── Luxury_Housing_Bangalore.csv       # Raw dataset (input)
│   └── luxury_housing_cleaned.csv         # Cleaned dataset (output)
│
├── scripts/
│   ├── data_cleaning_postgres.py          # Full pipeline script (cleaning + PG load)
│   └── sql_scripts.sql                    # Schema + validation + analytical queries
│
├── dashboard/
│   └── Luxury_Housing_Dashboard.pbix      # Power BI dashboard file
│
├── docs/
│   └── documentation.docx                 # Full project documentation report
│
└── README.md                              # This file
```

---

## Dataset Description

| Property | Detail |
|---|---|
| **File** | `Luxury_Housing_Bangalore.csv` |
| **Rows** | 1,01,000 |
| **Columns** | 18 raw → 26 after engineering |
| **Time Period** | Q1 FY2023 – Q1 FY2025 (9 quarters) |
| **Builders** | 11 (Prestige, Brigade, Sobha, Embassy, etc.) |
| **Micro-Markets** | 16 localities across Bengaluru |

### Key Columns

| Column | Type | Description |
|---|---|---|
| `Property_ID` | string | Unique property identifier |
| `Micro_Market` | string | Bengaluru locality (e.g. Whitefield, Koramangala) |
| `Developer_Name` | string | Builder name |
| `Ticket_Price_Cr` | float | Transaction price in Crores (mixed format in raw data) |
| `Configuration` | string | Unit type: 3BHK / 4BHK / 5BHK+ |
| `Amenity_Score` | float | Amenity quality score (0–10) |
| `Possession_Status` | string | Launch / Under Construction / Ready to Move |
| `Transaction_Type` | string | Primary (new booking) / Secondary (resale) |
| `Sales_Channel` | string | Broker / Direct / NRI Desk / Online |
| `Buyer_Type` | string | HNI / NRI / CXO / Startup Founder / Other |
| `Purchase_Quarter` | date | Transaction date (quarterly) |
| `Buyer_Comments` | string | Free-text feedback |

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.9+ | Data cleaning & pipeline |
| pandas | 2.x | Data manipulation |
| NumPy | 1.x | Numerical operations |
| SQLAlchemy | 2.x | ORM + DB connection |
| psycopg2-binary | 2.9+ | PostgreSQL adapter |
| PostgreSQL | 14+ | Data warehouse |
| Power BI Desktop | Latest | Dashboard & visualization |

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/luxury-housing-analysis.git
cd luxury-housing-analysis
```

### 2. Create a virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install pandas numpy sqlalchemy psycopg2-binary python-dotenv
```

### 4. Create the PostgreSQL database

Open your PostgreSQL client (pgAdmin or psql) and run:

```sql
CREATE DATABASE luxury_housing_db;
```

### 5. Configure credentials

Open `scripts/data_cleaning_postgres.py` and update the `DB_CONFIG` block:

```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "luxury_housing_db",
    "username": "postgres",        # ← your PostgreSQL username
    "password": "your_password",   # ← your PostgreSQL password
}
```

> **Tip:** For production, use a `.env` file instead of hardcoding credentials.
> See the commented-out `load_dotenv()` block in the script.

---

## Step 1 — Python: Data Cleaning & Feature Engineering

### Run the pipeline

```bash
python scripts/data_cleaning_postgres.py
```

### What it does

| Step | Action | Details |
|---|---|---|
| Load | Read raw CSV | 1,01,000 rows × 18 columns |
| Clean prices | Parse `Ticket_Price_Cr` | Handles `₹9.82 Cr`, plain floats, NaN |
| Normalize text | Title-case / uppercase | Micro_Market, Configuration, Builder |
| Handle nulls | Median imputation | Amenity_Score, Unit_Size_Sqft, Ticket_Price_Cr |
| Fill comments | Literal fill | `Buyer_Comments` → `"No Comment"` |
| Engineer features | Derive 8 new columns | See [Engineered Features](#engineered-features) |
| Rename columns | Match project spec | Property_ID → Project_ID, etc. |
| Save CSV | Backup output | `luxury_housing_cleaned.csv` |
| Load to PostgreSQL | `to_sql()` with chunks | 5,000 rows/chunk, `method='multi'` |
| Create indexes | 6 indexes | Builder, Micro_Market, Quarter_Label, etc. |
| Validate | 5 SQL checks | Row count, nulls, group-by checks |

### Expected output

```
[INFO] Loaded 101,000 rows × 18 columns
[INFO] Ticket_Price_Cr — nulls remaining: 0
[INFO] Text fields normalized.
[INFO] All nulls handled — dataset is complete.
[INFO] Feature engineering complete.
[INFO] PostgreSQL connection successful.
[INFO] Loading 101,000 rows to PostgreSQL → public.luxury_housing ...
[INFO] Successfully loaded to PostgreSQL table: public.luxury_housing
[INFO] Indexes created successfully.
[DONE] Full pipeline complete. Database is ready for Power BI.
```

---

## Step 2 — PostgreSQL: Load & Validate

The SQL scripts file (`sql_scripts.sql`) contains three sections:

### Section 1 — Table Schema

```sql
CREATE TABLE IF NOT EXISTS luxury_housing (
    Project_ID           VARCHAR(20) PRIMARY KEY,
    Micro_Market         VARCHAR(100) NOT NULL,
    Builder              VARCHAR(100) NOT NULL,
    Ticket_Price_Cr      DECIMAL(10,4),
    Configuration        VARCHAR(20),
    Amenity_Score        DECIMAL(5,4),
    Booking_Status       VARCHAR(50),
    Quarter_Label        VARCHAR(20),
    Booking_Flag         SMALLINT,
    ...
);
```

### Section 2 — Validation Queries

```sql
-- Total record count
SELECT COUNT(*) FROM luxury_housing;

-- Booking status split
SELECT Booking_Status, COUNT(*), ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct
FROM luxury_housing GROUP BY Booking_Status;

-- Avg ticket price per builder
SELECT Builder, ROUND(AVG(Ticket_Price_Cr),3) AS avg_price
FROM luxury_housing GROUP BY Builder ORDER BY avg_price DESC;
```

### Section 3 — Analytical Queries

All 10 Power BI visualization queries are pre-written. Run them in pgAdmin or pass them to Power BI via native query.

---

## Step 3 — Power BI: Dashboard & Visualizations

### Connect Power BI to PostgreSQL

1. Open **Power BI Desktop**
2. Click **Home → Get Data → PostgreSQL Database**
3. Enter:
   - **Server:** `localhost`
   - **Database:** `luxury_housing_db`
4. Select the `luxury_housing` table → **Load**

> If the PostgreSQL connector is missing, download it from:
> `File → Options → Preview Features → PostgreSQL connector`

### Slicers to add

Add these four slicers on every page for full interactivity:

| Slicer | Field |
|---|---|
| Builder | `Builder` |
| Micro-Market | `Micro_Market` |
| Quarter | `Quarter_Label` |
| Configuration | `Configuration` |

---

## Engineered Features

| New Column | Formula | Purpose |
|---|---|---|
| `Price_Per_Sqft_L` | `(Ticket_Price_Cr × 100) / Unit_Size_Sqft` | Price efficiency metric |
| `Year` | Extracted from `Purchase_Quarter` | Year-level filtering |
| `Quarter_Number` | 1–4 from `Purchase_Quarter` | Quarter sorting |
| `Quarter_Label` | `"Q2-2024"` format | X-axis label for charts |
| `Booking_Flag` | `1` if Primary, `0` if Secondary | KPI numerator |
| `NRI_Buyer_Flag` | `1` if NRI_Buyer = yes | NRI segment analysis |
| `Amenity_Tier` | Basic / Standard / Premium | Amenity impact segmentation |
| `Price_Segment` | `<5Cr / 5-10Cr / 10-15Cr / 15+Cr` | Pricing strategy buckets |

---

## Power BI Visualizations (10 Questions)

| # | Question | Chart Type | Key Fields |
|---|---|---|---|
| 1 | Quarterly booking trends across micro-markets? | Line Chart | X: `Quarter_Label`, Y: Count, Legend: `Micro_Market` |
| 2 | Which builders have highest total & avg ticket sales? | Bar Chart | `Builder` vs `SUM(Ticket_Price_Cr)`, `AVG(Ticket_Price_Cr)` |
| 3 | Correlation between amenity score & booking rate? | Scatter Plot | X: `Amenity_Score`, Y: `Booking_Conversion_Rate`, Size: Project Count |
| 4 | Micro-markets with highest/lowest conversion rates? | Stacked Column | `Micro_Market` vs `% Booking_Status` |
| 5 | Most in-demand housing configurations? | Donut Chart | `Configuration` vs Count |
| 6 | Which sales channels drive most successful bookings? | 100% Stacked Bar | `Sales_Channel` vs `Booking_Status` distribution |
| 7 | Which builders dominate each quarter? | Matrix Table | Rows: `Builder`, Cols: `Quarter_Label`, Values: `SUM(Ticket_Price_Cr)` |
| 8 | How does possession status affect buyer decisions? | Clustered Column | `Possession_Status` vs `Booking_Status`, colored by `Buyer_Type` |
| 9 | Where are luxury projects concentrated in Bengaluru? | Map Visual | Field: `Micro_Market` |
| 10 | Top 5 builders by revenue & booking success? | Card / KPI Visual | `Builder`, `Total_Revenue_Cr`, `Booking_Conversion_Rate` |

---

## DAX Measures

Add these in Power BI under **Modeling → New Measure**:

```dax
-- Booking conversion rate
Booking_Conversion_Rate =
DIVIDE(
    COUNTROWS(FILTER(luxury_housing, luxury_housing[Booking_Flag] = 1)),
    COUNTROWS(luxury_housing)
)

-- Total revenue in Crores
Total_Revenue_Cr = SUM(luxury_housing[Ticket_Price_Cr])

-- Average price per sqft
Avg_Price_Per_Sqft = AVERAGE(luxury_housing[Price_Per_Sqft_L])

-- Year-over-year revenue growth
YoY_Growth =
DIVIDE(
    [Total_Revenue_Cr] - CALCULATE([Total_Revenue_Cr],
        SAMEPERIODLASTYEAR(luxury_housing[Purchase_Quarter])),
    CALCULATE([Total_Revenue_Cr],
        SAMEPERIODLASTYEAR(luxury_housing[Purchase_Quarter]))
)

-- High amenity conversion rate
High_Amenity_Conversion =
CALCULATE([Booking_Conversion_Rate],
    luxury_housing[Amenity_Tier] = "Premium")
```

---

## Key Business Insights

### 🏘️ Market Intelligence
- All 16 micro-markets show near-equal demand (6,150–6,441 units), indicating a mature, evenly spread market
- **Jp Nagar** leads in volume (6,441 units) and **Sarjapur Road** is a close second
- **Kanakapura Road** has the highest primary booking conversion rate at **51.3%**

### 🏗️ Builder Performance
- **Prestige** leads total revenue at ₹1,17,810 Cr with a 50.7% primary conversion rate
- All 11 builders price within ₹0.20 Cr of each other (₹12.55–12.75 Cr avg), showing a price-disciplined competitive market

### 🎯 Amenity Impact
- Premium amenity tier achieves **50.06%** conversion vs **49.95%** for Standard — minimal difference
- Amenity score alone does not drive bookings; location and pricing are stronger drivers

### 🛏️ Configuration Demand
- **5BHK+** is the most booked configuration (33,876 units) closely followed by 3BHK and 4BHK
- Average price is nearly identical across all three configs, meaning larger units offer better price-per-sqft value

### 📣 Sales Channel Efficiency
- All 4 channels (Broker, Direct, NRI Desk, Online) convert at ~50%, showing no single dominant channel
- **NRI Desk** drives ~72% NRI buyer share vs ~18% for other channels

### 📅 Quarterly Trends
- Revenue is stable across all 9 quarters (~₹1,58,000–₹1,62,000 Cr per quarter)
- No significant seasonal pattern — demand is consistent year-round in Bengaluru luxury segment

---

## Deliverables Checklist

- [x] `data_cleaning_postgres.py` — Full Python cleaning + PostgreSQL load pipeline
- [x] `luxury_housing_cleaned.csv` — Cleaned dataset (101,000 rows × 26 columns)
- [x] `sql_scripts.sql` — Schema, validation queries, 10 analytical queries, DAX references
- [x] `documentation.docx` — Full project documentation report
- [x] Power BI Dashboard — 7 pages, 10 visualizations, 4 KPI cards, 4 slicers
- [x] `README.md` — This file

---

## Author

**Project submitted as part of GUVI × HCL Data Analytics Capstone**
Domain: Real Estate · Business Intelligence · Urban Market Research

---

> *"Data is the new real estate. Location, location, location — but now it's insights, insights, insights."*
