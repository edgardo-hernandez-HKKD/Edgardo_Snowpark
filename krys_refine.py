import utils as sf
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import FloatType, IntegerType
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session


# CONNECTION

# Start your Snowpark session
session = sf.create_snowpark_connection()
# Load your dataset into a DataFrame
refined_happiness = session.table("NEW_SNOWPARK.KRYS_RAW.RAW_HAPPINESS")
refined_happiness.show()
# TRANSFORMATIONS

# Rename Columns

def rename_columns(df):
    renamed_df = refined_happiness.rename({
        '"Country name"': "Country",
        '"year"': "Year",
        '"Life Ladder"': "Life_Satisfaction",
        '"Log GDP per capita"': "Economic_Prosperity",
        '"Social support"': "Social_Support",
        '"Healthy life expectancy at birth"': "Health_Life_Expectancy",
        '"Freedom to make life choices"': "Freedom",
        '"Generosity"': "Generosity",
        '"Perceptions of corruption"': "Corruption_Perception",
        '"Positive affect"': "Positive_Affect",
        '"Negative affect"': "Negative_Affect"
    })
    return renamed_df

# Drop Generosity
def drop_generosity(df):
    df_without_generosity = df.drop("Generosity")
    return df_without_generosity


# Check numeric values, no commas 
def check_commas(columns, df):
    for column in columns:
        if df.filter(col(column).like('%,%')).count() > 0:
            print(f"Commas found in numeric column: {column}")
            return
    print("All numeric columns are properly formatted (no commas)")

# Cast varchar to numeric
numeric_columns = ["LifeSatisfaction", "EconomicProsperity", "SocialSupport",
                       "HealthLifeExp", "Freedom", "CorruptionPercep",
                       "PositiveEmotions", "NegativeEmotions"]
year_column = 'Year'
def varchar_to_numeric(df):
    for column in numeric_columns:
        df = df.with_column(column, F.col(column).cast(FloatType()))          #Cast to float
    df = df.with_column(year_column, F.col(year_column).cast(IntegerType()))  #Cast to integer
    return df

# Check null values
def count_nulls_per_column(df):
    null_counts = {}
    for column in df.schema.names:
        null_count = df.filter(F.is_null(F.col(column))).count()
        null_counts[column] = null_count
    for column, count in null_counts.items():
        print(f"Column '{column}' has {count} null values.")

# IMPUTATION BY AVERAGE METHOD
def fill_nulls_by_country(df, columns_with_nulls):
    for column in columns_with_nulls:
        #Create new df with AVG by country and by specific column
        country_avg_df = df.group_by("Country").agg(F.avg(F.col(column)).alias(f"{column}_avg"))
        #Join the original df to new df 
        df = df.join(country_avg_df, "Country", "left")
        #Fill null values with AVG
        df = df.with_column(column, F.coalesce(F.col(column), F.col(f"{column}_avg")))
        #Deletes that column after using it
        df = df.drop(f"{column}_avg")
    return df

# CHANGE NAME OF COUNTRIES TO OFFICIAL NAMES
def replace_country_names(df):
    df = df.with_column(
        "Country",
        F.when(F.col("Country") == "Congo (Brazzaville)", "Republic of the Congo")
        .when(F.col("Country") == "Congo (Kinshasa)", "Democratic Republic of the Congo")
        .when(F.col("Country") == "Gambia", "The Gambia")
        .when(F.col("Country") == "Hong Kong S.A.R. of China", "Hong Kong")
        .when(F.col("Country") == "Myanmar", "Myanmar (Burma)")
        .when(F.col("Country") == "Somaliland region", "Republic of Somaliland")
        .when(F.col("Country") == "State of Palestine", "Palestine")
        .when(F.col("Country") == "Taiwan Province of China", "Taiwan")
        .otherwise(F.col("Country")))  #For other values, keep it that way
    return df


# TESTING FUNCTIONS ARE WORKING
#null_rows1 = (numeric_df.count()) - (numeric_df.na.drop().count())
#null_rows2 = (filled_df.count()) - (filled_df.na.drop().count())
#print(f"Rows with null values BEFORE IMPUTATION: {null_rows1}")
#print(f"Rows with null values AFTER IMPUTATION: {null_rows2}")
#count_nulls_per_column(cleaned_df)


def refine_data(session: Session):
    df = session.table('KRYS_RAW.RAW_HAPPINESS')
    df_without_generosity = drop_generosity(df)
    check_commas(numeric_columns, df_without_generosity)
    numeric_df = varchar_to_numeric(df_without_generosity)
    count_nulls_per_column(numeric_df)
    columns_with_nulls = ['EconomicProsperity', 'SocialSupport', 'HealthLifeExp', 
                      'Freedom', 'CorruptionPercep', 
                      'PositiveEmotions', 'NegativeEmotions']
    filled_df = fill_nulls_by_country(numeric_df, columns_with_nulls)
    cleaned_df = replace_country_names(filled_df)
    schema_name = "KRYS_REFINED"  
    table_name = "REFINED_WORLD_HAPPINESS"    
    cleaned_df.write.mode("overwrite").save_as_table(f"{schema_name}.{table_name}")
    print(f"DataFrame uploaded to table {table_name} on schema {schema_name}.") 



