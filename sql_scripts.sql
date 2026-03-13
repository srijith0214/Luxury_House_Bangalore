-- ============================================================
-- Luxury Housing Sales Analysis – Bengaluru
-- SQL Scripts: Schema + Validation + Analytical Queries
-- ============================================================


-- ─────────────────────────────────────────────
-- TABLE SCHEMA
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS luxury_housing (
    Project_ID            TEXT PRIMARY KEY,
    Micro_Market          TEXT,
    Project_Name          TEXT,
    Builder               TEXT,
    Unit_Size_Sqft        REAL,
    Configuration         TEXT,
    Ticket_Price_Cr       REAL,
    Booking_Status        TEXT,
    Buyer_Type            TEXT,
    Purchase_Quarter      TEXT,
    Connectivity_Score    REAL,
    Amenity_Score         REAL,
    Possession_Status     TEXT,
    Sales_Channel         TEXT,
    NRI_Buyer             TEXT,
    Locality_Infra_Score  REAL,
    Avg_Traffic_Time_Min  INTEGER,
    Buyer_Comments        TEXT,
    Price_Per_Sqft_L      REAL,
    Year                  INTEGER,
    Quarter_Number        INTEGER,
    Quarter_Label         TEXT,
    Booking_Flag          INTEGER,
    NRI_Buyer_Flag        INTEGER,
    Amenity_Tier          TEXT,
    Price_Segment         TEXT
);


-- ─────────────────────────────────────────────
-- VALIDATION QUERIES
-- ─────────────────────────────────────────────

-- V1: Total records
SELECT COUNT(*) AS Total_Records FROM luxury_housing;

-- V2: Records by Booking Status
SELECT Booking_Status, COUNT(*) AS Count
FROM luxury_housing
GROUP BY Booking_Status;

-- V3: Average Ticket Price per Builder
SELECT Builder,
       ROUND(AVG(Ticket_Price_Cr), 2) AS Avg_Price_Cr,
       COUNT(*) AS Total_Units
FROM luxury_housing
GROUP BY Builder
ORDER BY Avg_Price_Cr DESC;


-- ─────────────────────────────────────────────
-- ANALYTICAL QUERIES (matching Power BI questions)
-- ─────────────────────────────────────────────

-- Q1: Quarterly Booking Counts by Micro-Market (Market Trends)
SELECT Quarter_Label,
       Micro_Market,
       COUNT(*) AS Booking_Count,
       SUM(Booking_Flag) AS Primary_Bookings
FROM luxury_housing
GROUP BY Quarter_Label, Micro_Market
ORDER BY Quarter_Label, Micro_Market;


-- Q2: Builder Performance – Total & Avg Ticket Sales
SELECT Builder,
       ROUND(SUM(Ticket_Price_Cr), 2)  AS Total_Revenue_Cr,
       ROUND(AVG(Ticket_Price_Cr), 2)  AS Avg_Ticket_Size_Cr,
       COUNT(*) AS Units_Sold
FROM luxury_housing
GROUP BY Builder
ORDER BY Total_Revenue_Cr DESC;


-- Q3: Amenity Score vs Booking Conversion Rate
SELECT ROUND(Amenity_Score, 1) AS Amenity_Score_Bucket,
       COUNT(*) AS Total_Units,
       SUM(Booking_Flag) AS Primary_Bookings,
       ROUND(100.0 * SUM(Booking_Flag) / COUNT(*), 2) AS Conversion_Rate_Pct
FROM luxury_housing
GROUP BY Amenity_Score_Bucket
ORDER BY Amenity_Score_Bucket;


-- Q4: Booking Conversion Rate by Micro-Market
SELECT Micro_Market,
       COUNT(*) AS Total_Inquiries,
       SUM(Booking_Flag) AS Booked,
       ROUND(100.0 * SUM(Booking_Flag) / COUNT(*), 2) AS Conversion_Rate_Pct
FROM luxury_housing
GROUP BY Micro_Market
ORDER BY Conversion_Rate_Pct DESC;


-- Q5: Configuration Demand (most popular unit types)
SELECT Configuration,
       COUNT(*) AS Total_Units,
       SUM(Booking_Flag) AS Bookings,
       ROUND(100.0 * SUM(Booking_Flag) / COUNT(*), 2) AS Booking_Rate_Pct
FROM luxury_housing
GROUP BY Configuration
ORDER BY Bookings DESC;


-- Q6: Sales Channel Efficiency
SELECT Sales_Channel,
       COUNT(*) AS Total_Leads,
       SUM(Booking_Flag) AS Conversions,
       ROUND(100.0 * SUM(Booking_Flag) / COUNT(*), 2) AS Conversion_Rate_Pct
FROM luxury_housing
GROUP BY Sales_Channel
ORDER BY Conversion_Rate_Pct DESC;


-- Q7: Quarterly Builder Contribution (Revenue Matrix)
SELECT Builder,
       Quarter_Label,
       ROUND(SUM(Ticket_Price_Cr), 2) AS Revenue_Cr
FROM luxury_housing
GROUP BY Builder, Quarter_Label
ORDER BY Builder, Quarter_Label;


-- Q8: Possession Status vs Buyer Type and Booking Decisions
SELECT Possession_Status,
       Buyer_Type,
       Booking_Status,
       COUNT(*) AS Count
FROM luxury_housing
GROUP BY Possession_Status, Buyer_Type, Booking_Status
ORDER BY Possession_Status, Buyer_Type;


-- Q9: Project Concentration by Micro-Market (Geographical)
SELECT Micro_Market,
       COUNT(DISTINCT Project_Name) AS Project_Count,
       COUNT(*) AS Unit_Count,
       ROUND(AVG(Ticket_Price_Cr), 2) AS Avg_Price_Cr
FROM luxury_housing
GROUP BY Micro_Market
ORDER BY Unit_Count DESC;


-- Q10: Top 5 Builders by Revenue & Booking Success
SELECT Builder,
       ROUND(SUM(Ticket_Price_Cr), 2) AS Total_Revenue_Cr,
       ROUND(100.0 * SUM(Booking_Flag) / COUNT(*), 2) AS Booking_Success_Pct,
       COUNT(*) AS Total_Units
FROM luxury_housing
GROUP BY Builder
ORDER BY Total_Revenue_Cr DESC
LIMIT 5;


-- BONUS: Price per Sqft by Micro-Market
SELECT Micro_Market,
       ROUND(AVG(Price_Per_Sqft_L), 2) AS Avg_Price_Per_Sqft_L,
       ROUND(MIN(Ticket_Price_Cr), 2)  AS Min_Price_Cr,
       ROUND(MAX(Ticket_Price_Cr), 2)  AS Max_Price_Cr
FROM luxury_housing
GROUP BY Micro_Market
ORDER BY Avg_Price_Per_Sqft_L DESC;


-- BONUS: NRI vs Local Buyer Breakdown
SELECT Buyer_Type,
       NRI_Buyer,
       COUNT(*) AS Count,
       ROUND(AVG(Ticket_Price_Cr), 2) AS Avg_Price_Cr
FROM luxury_housing
GROUP BY Buyer_Type, NRI_Buyer
ORDER BY Buyer_Type;
