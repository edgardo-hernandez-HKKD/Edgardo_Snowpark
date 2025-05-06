from snowflake.snowpark import Session
import utils as sf

# Establish the Snowflake session
session = sf.create_snowpark_connection()

# Setup: warehouse, database, schema
session.sql("USE WAREHOUSE COMPUTE_WH").collect()
session.use_database("NEW_SNOWPARK")
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.EDO_RAW").collect()
session.sql("CREATE STAGE IF NOT EXISTS NEW_SNOWPARK.EDO_RAW.EDO_STAGE").collect()
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.EDO_REFINED").collect()
session.sql("CREATE SCHEMA IF NOT EXISTS NEW_SNOWPARK.EDO_CURATED").collect()

session.use_schema("EDO_RAW")
session.sql('select current_warehouse(), current_database(), current_schema(), current_user(), current_role()').collect()

# File Format
session.sql("""
    CREATE OR REPLACE FILE FORMAT EDO_CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
""").collect()

# Stage
session.sql("""
    CREATE OR REPLACE STAGE V_STAGE 
    FILE_FORMAT = EDO_CSV_FILE_FORMAT
""").collect()

# Load Data
file_path = '/Users/edgardo_hernandez/Documents/Projects/Snowpark/WorldHapinessReport/WorldHappinessReport2024.csv'
session.file.put(file_path, 'EDO_STAGE', auto_compress=False)

session.sql("LIST @EDO_STAGE").show()

# Create table 
session.sql("""
    CREATE OR REPLACE TABLE NEW_SNOWPARK.EDO_RAW.RAW_HAPPINESS (
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
    COPY INTO NEW_SNOWPARK.EDO_RAW.RAW_HAPPINESS
    FROM @EDO_STAGE
    FILE_FORMAT = EDO_CSV_FILE_FORMAT
    ON_ERROR = 'CONTINUE'
""").collect()

# Check the data
raw_happiness = session.table("NEW_SNOWPARK.EDO_RAW.RAW_HAPPINESS")
raw_happiness.show()



