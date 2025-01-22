import os
from datetime import datetime
from dotenv import load_dotenv

from airflow.decorators import dag
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Variable, Pool
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

from include.glue_job_submission import create_glue_job
from include.utils.mortgage_data_access import get_year_quarter_release, check_for_data_release
from include.utils.trino_queries import execute_trino_query, run_trino_query_dq_check
from include.load_trino_into_snowflake import load_into_snowflake


# Define target tables
prod_table_origination = 'giulio_bellini.raw_mortgage_origination'
prod_table_performance = 'giulio_bellini.raw_mortgage_performance'
# Define audit tables
audit_origination_table = 'giulio_bellini.audit_mortgage_origination'
audit_performance_table = 'giulio_bellini.audit_mortgage_performance'

# Get secrets from AWS Secrets Manager
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
# Define local script for Glue Job
local_script_path = os.path.join("include", 'scripts/glue_iceberg_load.py')

# Get Freddie Mac credentials from AWS Secrets Manager
FM_USERNAME = Variable.get("FM_USERNAME")
FM_PASSWORD = Variable.get("FM_PASSWORD")

# DBT project configurations
# Load environment variables from .env file
dbt_env_path = os.path.join('dbt_project','dbt.env')
load_dotenv(dbt_env_path)
# Define path to DBT project and profiles.yml file
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
PATH_TO_DBT_PROJECT = os.path.join(AIRFLOW_HOME, 'dbt_project')
PATH_TO_DBT_PROFILES = os.path.join(AIRFLOW_HOME, 'dbt_project/profiles.yml')
# Define dbt profile configuration
profile_config = ProfileConfig(
    profile_name="freddie_mac",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)



@dag(
    "fm_load_dag",
    description="A DAG to load mortgage data from Freddie Mac Single Family Loan-Level Dataset into Iceberg tables.",
    default_args={
        "owner": "Giulio Bellini",
        "start_date": datetime(2024, 12, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=1)
    },
    max_active_runs=1,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath='include',
    tags=["gb2412"],
    params={
        "START_YEAR": Param(
            2015,
            type= "integer",
            description= "The year from which to start loading data."
        ),
    }
)

def gb2412_fm_load_dag():

    # Create a pool with 1 slot to limit the number of running tasks
    # Avoid memory issues in Trino and concurrent writes in Iceberg
    single_task_pool = Pool.create_or_update_pool(
        "single_task_pool",
        slots=1,
        description="Limit to 1 running task at a time",
        include_deferred=False
    )

    # DummyOperator to start the DAG
    start_execution = DummyOperator(
        task_id='start_execution'
    )

    # Get the most recent year and quarter available for download
    get_year_quarter_realease = PythonOperator(
        task_id="get_year_quarter_release",
        python_callable=get_year_quarter_release,
        op_kwargs={
            'username': FM_USERNAME,
            'password': FM_PASSWORD},
    )

    # Function to create origination data table
    def create_orig_table(table):
        return f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    Credit_Score INT,
                    First_Payment_Month VARCHAR,
                    First_Time_Home_Buyer_Flag VARCHAR,
                    Maturity_Date VARCHAR,
                    Metropolitan_Area VARCHAR,
                    MI_Percentage DOUBLE,
                    Num_Units INT,
                    Occupancy_Status VARCHAR,
                    CLTV DOUBLE,
                    DTI DOUBLE,
                    UPB BIGINT,
                    LTV DOUBLE,
                    Int_Rate DOUBLE,
                    Channel VARCHAR,
                    Prepay_Penalty VARCHAR,
                    Amortization_Type VARCHAR,
                    Property_State VARCHAR,
                    Property_Type VARCHAR,
                    ZIP_Code VARCHAR,
                    Loan_Num VARCHAR,
                    Loan_Purpose VARCHAR,
                    Loan_Term INT,
                    Num_Borrowers INT,
                    Seller_Name VARCHAR,
                    Servicer_Name VARCHAR,
                    Sup_Conf_Flag VARCHAR,
                    Pre_Ref_Loan_Num VARCHAR,
                    Program_Indicator VARCHAR,
                    Ref_Indicator VARCHAR,
                    Prop_Val_Method INT,
                    Int_Only_Indicator VARCHAR,
                    MI_Canc_Indicator VARCHAR,
                    year INT,
                    quarter INT,
                    date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['year','First_Payment_Month']
                    )
                """

    # Function to create performance data table
    def create_perf_table(table):
        return f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    Loan_Num VARCHAR,
                    Month_Reporting VARCHAR,
                    Current_Actual_UPB DOUBLE,
                    Current_Delinquency_Status VARCHAR,
                    Loan_Age DOUBLE,
                    Remaining_Months_To_Legal_Maturity DOUBLE,
                    Defect_Settlement_Date VARCHAR,
                    Modification_Flag VARCHAR,
                    Zero_Balance_Code VARCHAR,
                    Zero_Balance_Effective_Date VARCHAR,
                    Current_Int_Rate DOUBLE,
                    Current_Non_Int_UPB DOUBLE,
                    DDLPI VARCHAR,
                    MI_Recoveries DOUBLE,
                    Net_Sale_Proceeds DOUBLE,
                    Non_MI_Recoveries DOUBLE,
                    Total_Expenses DOUBLE,
                    Legal_Costs DOUBLE,
                    Maintenance_And_Preservation_Costs DOUBLE,
                    Taxes_And_Insurance DOUBLE,
                    Miscellaneous_Expenses DOUBLE,
                    Actual_Loss DOUBLE,
                    Cum_Modification_Cost DOUBLE,
                    Step_Modification_Flag VARCHAR,
                    Payment_Deferral VARCHAR,
                    ELTV DOUBLE,
                    Zero_Bal_Removal_UPB DOUBLE,
                    Delinquent_Accrued_Int DOUBLE,
                    Disaster_Delinq_Flag VARCHAR,
                    Borrower_Assistance_Status_Code VARCHAR,
                    Curr_Month_Modification_Cost DOUBLE,
                    Curr_Int_UPB DOUBLE,
                    year INT,
                    quarter INT,
                    date DATE
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['year','Month_Reporting']
                    )
                """

    # Create Iceberg tables for origination and performance data
    with TaskGroup(group_id="create_prod_tables") as prod_tables_group:

        create_prod_origination_table = PythonOperator(
            task_id="create_prod_origination_table",
            python_callable=execute_trino_query,
            op_kwargs={
                'query': create_orig_table(prod_table_origination)
            }
        )

        create_prod_performance_table = PythonOperator(
            task_id="create_prod_performance_table",
            python_callable=execute_trino_query,
            op_kwargs={
                'query': create_perf_table(prod_table_performance)
            }
        )

        create_prod_origination_table >> create_prod_performance_table


    # Get the most recent year and quarter in the origination table
    get_year_quarter_table = PythonOperator(
        task_id="get_year_quarter_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            SELECT DISTINCT 
                year, quarter 
            FROM {prod_table_origination}
            ORDER BY year DESC, quarter DESC
            LIMIT 1
            """
        }
    )

    # Check if new data has been released
    def check_for_data_release_wrapper(**context):
        # Get taskinstance object from context
        ti = context['ti']
        # Get start year from params
        start_year = context['params']['START_YEAR']
        return check_for_data_release(ti, start_year)

    check_for_release = PythonOperator(
        task_id="check_for_release",
        python_callable=check_for_data_release_wrapper,
        provide_context=True
    )

    # Branching based on data release
    new_data_released = ShortCircuitOperator(
        task_id="new_data_released",
        python_callable=lambda ti: bool(ti.xcom_pull(task_ids='check_for_release'))
    )

    # Get the list of years and quarters to load
    # Only most recent quarter per year returned 
    # (e.g. (2024,4) -> (2024,1),(2024,2),(2024,3),(2024,4))
    def get_years_quarters_list(**context):
        ti = context['ti']
        years_quarters_list = ti.xcom_pull(task_ids='check_for_release')
        print('Years and Quarters List:', years_quarters_list)
        return years_quarters_list

    get_years_quarters = PythonOperator(
        task_id="get_years_quarters_list",
        python_callable=get_years_quarters_list,
        provide_context=True,
    )

    # Create and clear audit origination table
    with TaskGroup(group_id="create_clear_audit_origination") as audit_origination_group:

        create_audit_origination_table = PythonOperator(
            task_id="create_audit_origination_table",
            python_callable=execute_trino_query,
            op_kwargs={
                'query': create_orig_table(audit_origination_table)
            }
        )

        clear_audit_origination_table = PythonOperator(
            task_id="clear_audit_origination_table",
            python_callable=execute_trino_query,
            op_kwargs={
                        'query': """DELETE FROM {audit_origination_table}
                                    WHERE year in ({year_list})
                                """.format(
                                    audit_origination_table=audit_origination_table,
                                    year_list = 
                                    '''{%- for year,_ in ti.xcom_pull(task_ids='get_years_quarters_list') -%}
                                            {{ year }}
                                        {%- if not loop.last -%}, {% endif -%}
                                        {%- endfor -%}'''
                                )
                    }
        )

        create_audit_origination_table >> clear_audit_origination_table

    # Create and clear audit performance table
    with TaskGroup(group_id="create_clear_audit_performance") as audit_performance_group:
        create_audit_performance_table = PythonOperator(
            task_id="create_audit_performance_table",
            python_callable=execute_trino_query,
            op_kwargs={
                'query': create_perf_table(audit_performance_table)
            }
        )

        clear_audit_performance_table = PythonOperator(
            task_id="clear_audit_performance_table",
            python_callable=execute_trino_query,
            op_kwargs={
                        'query': """DELETE FROM {audit_performance_table}
                                    WHERE year in ({year_list})
                                """.format(
                                    audit_performance_table=audit_performance_table,
                                    year_list = 
                                    '''{%- for year,_ in ti.xcom_pull(task_ids='get_years_quarters_list') -%}
                                            {{ year }}
                                        {%- if not loop.last -%}, {% endif -%}
                                        {%- endfor -%}'''
                                )
                    }
        )

        create_audit_performance_table >> clear_audit_performance_table
    
    # Load data into audit tables
    # Spin off a Glue Job for each year
    # Process years sequentially to avoid concurrent writes in Iceberg
    load_to_audit_tables = PythonOperator.partial(
        task_id="load_to_audit_tables",
        python_callable=create_glue_job,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=4),
        pool=single_task_pool.pool,
        map_index_template="job_name={{ task.op_kwargs['job_name'] }}"
    ).expand_kwargs(
        get_years_quarters.output.map(
            lambda year_quarter: {
                'op_kwargs': {
                    'job_name': f'fm_data_load_{year_quarter[0]}',
                    'description': 'Upload Freddie Mac data into iceberg table',
                    'script_path':local_script_path,
                    "aws_access_key_id": aws_access_key_id,
                    "aws_secret_access_key": aws_secret_access_key,
                    "tabular_credential": tabular_credential,
                    "s3_bucket": s3_bucket,
                    "catalog_name": catalog_name,
                    "aws_region": aws_region,
                    'arguments':{
                        '--ds': '{ds}'.format(ds='{{ ds }}'), 
                        '--origination_table': f'{audit_origination_table}',
                        '--performance_table': f'{audit_performance_table}'
                    },
                    'fm_credentials': {
                        '--fm_username': FM_USERNAME,
                        '--fm_password': FM_PASSWORD
                    },
                    'year_quarter': {
                        '--year': str(year_quarter[0]),
                        '--last_quarter': str(year_quarter[1])
                    }
                }
            }
        )
    )

    # Run DQ checks on audit tables
    # No data should be removed from production tables
    with TaskGroup(group_id="run_dq_checks") as run_dq_checks:

        run_dq_check_origination = PythonOperator(
            task_id="run_dq_check_origination",
            python_callable=run_trino_query_dq_check,
            retries=2,
            retry_delay=timedelta(minutes=2),
            pool=single_task_pool.pool,
            op_kwargs={
                    'query': """
                            WITH dq_check AS (
                                SELECT
                                    r.year,
                                    r.loan_num
                                FROM
                                    {prod_table} r
                                LEFT JOIN
                                    {audit_table} a 
                                        ON r.loan_num = a.loan_num
                                WHERE
                                    a.loan_num IS NULL AND r.year IN ({year_list})
                            )
                            SELECT
                                year,
                                COUNT(*) as num_exceptions
                            FROM
                                dq_check
                            GROUP BY
                                year
                        """.format(
                            prod_table = prod_table_origination,
                            audit_table = audit_origination_table,
                            year_list = 
                            '''{%- for year in ti.xcom_pull(task_ids='load_to_audit_tables') -%}
                                    {{ year }}
                                {%- if not loop.last -%}, {% endif -%}
                                {%- endfor -%}'''
                        )
                }
        )

        run_dq_check_performance = PythonOperator.partial(
            task_id="run_dq_check_performance",
            python_callable=run_trino_query_dq_check,
            retries=2,
            retry_delay=timedelta(minutes=5),
            pool=single_task_pool.pool,
        ).expand_kwargs(
            get_years_quarters.output.map(
                lambda year_quarter: {
                    'op_kwargs': {
                            'query': f"""
                                WITH dq_check AS (
                                    SELECT
                                        r.year,
                                        r.loan_num,
                                        r.month_reporting
                                    FROM
                                        {prod_table_performance} r
                                    LEFT JOIN
                                        {audit_performance_table} a 
                                            ON r.loan_num = a.loan_num AND r.month_reporting = a.month_reporting
                                    WHERE
                                        a.loan_num IS NULL AND r.year = {year_quarter[0]}
                                )
                                SELECT
                                    year,
                                    COUNT(*) as num_exceptions
                                FROM
                                    dq_check
                                GROUP BY
                                    year
                            """
                    }
                }
            )
        )

        run_dq_check_origination >> run_dq_check_performance

    # Clear and move data from audit to production origination table
    with TaskGroup(group_id="clear_move_prod_origination") as prod_origination_group:

        clear_prod_origination_table = PythonOperator(
            task_id="clear_prod_origination_table",
            python_callable=execute_trino_query,
            op_kwargs={
                        'query': """DELETE FROM {prod_table_origination}
                                    WHERE 
                                        year in ({year_list})
                                """.format(
                                    prod_table_origination=prod_table_origination,
                                    year_list = 
                                    '''{%- for year in ti.xcom_pull(task_ids='load_to_audit_tables') -%}
                                            {{ year }}
                                        {%- if not loop.last -%}, {% endif -%}
                                        {%- endfor -%}'''
                                )
                    }
        )

        move_to_prod_origination = PythonOperator(
            task_id="exchange_data_from_audit_to_prod_origination",
            python_callable=execute_trino_query,
            pool=single_task_pool.pool,
            op_kwargs={
                'query': """
                        INSERT INTO {prod_table_origination}
                        SELECT * FROM {audit_origination_table}
                        WHERE
                            year in ({year_list})
                        """.format(
                            prod_table_origination=prod_table_origination,
                            audit_origination_table=audit_origination_table,
                            year_list = 
                            '''{%- for year in ti.xcom_pull(task_ids='load_to_audit_tables') -%}
                                    {{ year }}
                                {%- if not loop.last -%}, {% endif -%}
                                {%- endfor -%}'''
                    )
                }
        )

        clear_prod_origination_table >> move_to_prod_origination
    
    # Clear and move data from audit to production performance table
    with TaskGroup(group_id="clear_move_prod_performance") as prod_performance_group:
        
        clear_prod_performance_table = PythonOperator(
            task_id="clear_prod_performance_table",
            python_callable=execute_trino_query,
            op_kwargs={
                        'query': """DELETE FROM {prod_table_performance}
                                    WHERE 
                                        year in ({year_list})
                                """.format(
                                    prod_table_performance=prod_table_performance,
                                    year_list = 
                                    '''{%- for year in ti.xcom_pull(task_ids='load_to_audit_tables') -%}
                                            {{ year }}
                                        {%- if not loop.last -%}, {% endif -%}
                                        {%- endfor -%}'''
                                )
                    }
        )

        move_to_prod_performance = PythonOperator.partial(
            task_id="exchange_data_from_audit_to_prod_performance",
            python_callable=execute_trino_query,
            pool=single_task_pool.pool,
            ).expand_kwargs(
            load_to_audit_tables.output.map(
                lambda year: {
                    'op_kwargs': {
                        'query': f"""
                            INSERT INTO {prod_table_performance}
                            SELECT * FROM {audit_performance_table}
                            WHERE
                                year = {year}
                            """
                    }
                }
            )
        )


        clear_prod_performance_table >> move_to_prod_performance
    
    # Run dbt project to transform data
    run_dbt_transformation = DbtTaskGroup(
        group_id="mortgage_data_transformation",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        operator_args={
            "pool": single_task_pool.pool,
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(hours=1)
        }
    )

    # Load visualization tables into Snowflake
    load_viz_tables_into_snowflake = PythonOperator(
            task_id="load_viz_tables_into_snowflake",
            python_callable=load_into_snowflake,
            retries = 3,
            retry_delay=timedelta(minutes=5),
            op_kwargs={
                'snow_schema': 'giulio_bellini',
                'table_list': ['giulio_bellini.m__interest', 
                               'giulio_bellini.m__loss', 
                               'giulio_bellini.m__loans_number', 
                               'giulio_bellini.m__principal',
                               'giulio_bellini.m__portfolio_risk']
            }
        )

    # DummyOperator to end the DAG
    end_execution = DummyOperator(
        task_id='end_execution',
        trigger_rule='none_failed'
    )


    # Define DAG execution flow
    (start_execution
     >> get_year_quarter_realease
     >> prod_tables_group
     >> get_year_quarter_table
     >> check_for_release >> new_data_released
     >> get_years_quarters
     >> audit_origination_group >> audit_performance_group
     >> load_to_audit_tables
     >> run_dq_checks
     >> prod_origination_group >> prod_performance_group
     >> run_dbt_transformation
     >> load_viz_tables_into_snowflake
     >> end_execution
     )

gb2412_fm_load_dag()
