# IMPORT LIBRARIES
import utils as sf  
from snowflake.snowpark import Session    
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, count, is_null, when

# CONNECTION
session = sf.create_snowpark_connection()                     

# Start your Snowpark session
session = sf.create_snowpark_connection()           

# Load your dataset into a DataFrame
refined_happiness = session.table("NEW_SNOWPARK.KRYS_RAW.WORLD_HAPPINESS_DATA_TABLE")

# Change columns names
refined_happiness = refined_happiness.rename(col("Country name"), "Country") 

refined_happiness.show() 



