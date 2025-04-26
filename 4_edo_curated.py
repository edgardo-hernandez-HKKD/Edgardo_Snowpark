from snowflake.snowpark import Session
import utils as sf

# Establish the Snowflake session
session = sf.create_snowpark_connection()

# Access the refined health data table in EDO_REFINED
refined_health_data = session.table("EDO_REFINED.REFINED_HEALTH_DATA")

# Create View 1: Contains Person Id, Age, and Sleep Disorder
person_age_sleep_view = refined_health_data.select(
    "PERSON_ID", 
    "AGE", 
    "SLEEP_DISORDER"
)
person_age_sleep_view.create_or_replace_view(["EDO_CURATED", "VIEW_PERSON_AGE_SLEEP"])
print("View VIEW_PERSON_AGE_SLEEP created successfully in EDO_CURATED.")

# Create View 2: Contains Gender, Sleep Disorder, Sleep Duration, Quality of Sleep, and Stress Level
gender_sleep_details_view = refined_health_data.select(
    "GENDER", 
    "SLEEP_DISORDER", 
    "SLEEP_DURATION", 
    "QUALITY_OF_SLEEP", 
    "STRESS_LEVEL"
)
gender_sleep_details_view.create_or_replace_view(["EDO_CURATED", "VIEW_GENDER_SLEEP_DETAILS"])
print("View VIEW_GENDER_SLEEP_DETAILS created successfully in EDO_CURATED.")

# Create View 3: Contains Occupation, Sleep Duration, Quality of Sleep, Stress Level, Sleep Disorder, and Age
occupation_sleep_details_view = refined_health_data.select(
    "OCCUPATION", 
    "SLEEP_DURATION", 
    "QUALITY_OF_SLEEP", 
    "STRESS_LEVEL", 
    "SLEEP_DISORDER", 
    "AGE"
)
occupation_sleep_details_view.create_or_replace_view(["EDO_CURATED", "VIEW_OCCUPATION_SLEEP_DETAILS"])
print("View VIEW_OCCUPATION_SLEEP_DETAILS created successfully in EDO_CURATED.")



