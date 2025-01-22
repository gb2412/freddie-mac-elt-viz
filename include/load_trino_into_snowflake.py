import sys
import os
from utils.trino_queries import execute_trino_query
from utils.snowflake_queries import get_snowpark_session


def load_into_snowflake(table_list, snow_schema):
    """
    Load data from Trino into Snowflake.

    Parameters:
        - table_list (list): List of table names to load into Snowflake.
        - snow_schema (str): Snowflake schema to load the tables into.
    """
    # Define Snowflake connection parameters
    # Create a Snowpark session
    snowflake_session = get_snowpark_session(snow_schema)
    for table in table_list:
        data = execute_trino_query(f'SELECT * FROM {table}')
        schema = execute_trino_query(f'DESCRIBE {table}')
        columns = []
        column_names = []
        for column in schema:
            column_names.append(column[0])
            columns.append(' '.join(column))
        current_config = snowflake_session.sql("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
        print(f"Using warehouse: {current_config[0][0]}, database: {current_config[0][1]}, schema: {current_config[0][2]}")
        columns_str = ','.join(columns)
        create_ddl = f'CREATE TABLE IF NOT EXISTS {table} ({columns_str})'
        snowflake_session.sql(create_ddl)
        write_df = snowflake_session.create_dataframe(data, schema=column_names)
        write_df.write.mode("overwrite").save_as_table(table)
    snowflake_session.close()

