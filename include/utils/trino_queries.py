import os
from dotenv import load_dotenv

import trino

load_dotenv()

def run_trino_query_dq_check(query):
    """
    DQ checks: no exceptions should be returned from the query.

    Parameters:
        - query (str): Query to execute.
    """
    results = execute_trino_query(query)
    if len(results) == 0:
        print('PASS: No exceptions returned')
    else:
        raise ValueError(f'FAIL: Query returned {results}')


def execute_trino_query(query):

    host = os.getenv('TRINO_HOST')
    port = os.getenv('TRINO_PORT', 443)
    user = os.getenv('TRINO_USER')
    password = os.getenv('TRINO_PASSWORD')
    catalog = os.getenv('TRINO_CATALOG')

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        http_scheme='https',
        catalog=catalog,
        auth=trino.auth.BasicAuthentication(user, password),
    )
    print(query)
    cursor = conn.cursor()
    print("Executing query for the first time...")
    cursor.execute(query)
    return cursor.fetchall()