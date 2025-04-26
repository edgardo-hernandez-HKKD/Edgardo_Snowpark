import snowflake.snowpark as snowpark
import json

def read_creds_from_json():
    try:
        with open('credentials.json') as f:
            connection_parameters = json.load(f)
    except Exception as e:
        raise ValueError(f'Error reading JSON file.') from e
    return connection_parameters


def create_snowpark_connection():
    creds = read_creds_from_json()
    return snowpark.Session.builder.configs(creds).create() 


read_creds_from_json()
create_snowpark_connection()