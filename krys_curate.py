import utils as sf
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session

# 2.1. VIEW 1. ONLY 2010-2023 YEARS
# For data availability, coverage consistency and temporal relevance
def years_14(df):
    df_2010_2023 = df.filter(col("Year") >= 2010)
    return df_2010_2023

# 2.2. VIEW 2. GROUP BY COUNTRY AND PERFORM AVERAGE TO VALUES
def group_by_country(df):
    grouped_df = (df.group_by("Country").agg(
            F.avg("Life_Satisfaction").alias("Life_Satisfaction"),
            F.avg("Economic_Prosperity").alias("Economic_Prosperity"),
            F.avg("Social_Support").alias("Social_Support"),
            F.avg("Health_Life_Expectancy").alias("Health_Life_Expectancy"),
            F.avg("Freedom").alias("Freedom"),
            F.avg("Corruption_Perception").alias("Corruption_Perception"),
            F.avg("Positive_Affect").alias("Positive_Affect"),
            F.avg("Negative_Affect").alias("Negative_Affect")))
    return grouped_df

# 2.3. CREATE VIEWS
view_name_2010_2023 = "WORLD_HAPPINESS_2010_2023"         # VIEW 1
view_name_average_df = "COUNTRY_AVERAGE_WORLD_HAPPINESS"  # VIEW 2

def cleaned_data(session: Session):
    cleaned_df = session.table('KRYS_REFINED.REFINED_WORLD_HAPPINESS')
    df_2010_2023 = years_14(cleaned_df)
    average_df = group_by_country(df_2010_2023)
    session.use_database("NEW_SNOWPARK")
    session.use_schema("KRYS_CURATED")
    df_2010_2023.create_or_replace_view(view_name_2010_2023)
    average_df.create_or_replace_view(view_name_average_df)
    print(f"Views '{view_name_2010_2023}' and '{view_name_average_df}' successfully created.")


if __name__ == "__main__":
    session = sf.create_snowpark_connection()
    cleaned_data(session)
