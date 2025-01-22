import os
from dotenv import load_dotenv

import snowflake.connector
from snowflake.snowpark import Session

# Load environment variables
load_dotenv()

connection_params = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "role": os.getenv('SNOWFLAKE_ROLE'),
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "database": os.getenv('SNOWFLAKE_DATABASE'),
    }

def get_snowpark_session(schema):
    connection_params['schema'] = schema
    session = Session.builder.configs(connection_params).create()
    return session


def run_snowflake_query_dq_check(query):
    results = execute_snowflake_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True


def execute_snowflake_query(query):
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(**connection_params)
    try:
        # Create a cursor object to execute queries
        cursor = conn.cursor()
        # Example query: Get the current date from Snowflake
        cursor.execute(query)
        # Fetch and print the result
        result = cursor.fetchall()
        return result
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()