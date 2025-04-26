from snowflake.snowpark import Session
import utils as sf


# Establish the Snowflake session
session = sf.create_snowpark_connection()

# Setup: warehouse, database, schema
session.sql("USE WAREHOUSE COMPUTE_WH").collect()
session.sql("CREATE DATABASE IF NOT EXISTS NEW_SNOWPARK").collect()
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.KRYS_RAW").collect()
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.KRYS_REFINED").collect()
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.KRYS_CURATED").collect()

session.use_database("NEW_SNOWPARK")
session.use_schema("KRYS_RAW")
session.sql('select current_warehouse(), current_database(), current_schema(), current_user(), current_role()').collect()

# File Format
session.sql("""
    CREATE OR REPLACE FILE FORMAT KRYS_CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
""").collect()

# Stage
session.sql("""
    CREATE OR REPLACE STAGE KRYS_STAGE 
    FILE_FORMAT = KRYS_CSV_FILE_FORMAT
""").collect()

# Load Data
file_path = '/Users/edgardo_hernandez/Documents/Projects/Snowpark/WorldHapinessReport/WorldHappinessReport2024.csv'
session.file.put(file_path, 'KRYS_STAGE', auto_compress=False)

session.sql("LIST @KRYS_STAGE").show()

# Create table 
session.sql("""
    CREATE OR REPLACE TABLE NEW_SNOWPARK.KRYS_RAW.RAW_HAPPINESS (
        "Country name" STRING,
        "year" STRING,
        "Life Ladder" STRING,
        "Log GDP per capita" STRING,
        "Social support" STRING,
        "Healthy life expectancy at birth" STRING,
        "Freedom to make life choices" STRING,
        "Generosity" STRING,
        "Perceptions of corruption" STRING,
        "Positive affect" STRING,
        "Negative affect" STRING
    )
""").collect()

# Load data into table
session.sql("""
    COPY INTO NEW_SNOWPARK.KRYS_RAW.RAW_HAPPINESS
    FROM @KRYS_STAGE
    FILE_FORMAT = KRYS_CSV_FILE_FORMAT
    ON_ERROR = 'CONTINUE'
""").collect()

# Check the data
raw_happiness = session.table("NEW_SNOWPARK.KRYS_RAW.RAW_HAPPINESS")
raw_happiness.show()



