import sys
import time
import zipfile
import io
import gc
import itertools
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit


# Initialize Spark and Glue sessions
spark = (SparkSession.builder.getOrCreate())
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", 
                                     "ds", 
                                     "origination_table",
                                     "performance_table",
                                     "year",
                                     "last_quarter",
                                     "fm_username",
                                     "fm_password"])
run_date = args['ds']
origination_table = args['origination_table']
performance_table = args['performance_table']
year = int(args['year'])
last_quarter = int(args['last_quarter'])
FM_USERNAME = args['fm_username']
FM_PASSWORD = args['fm_password']

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# URLs
login_page_url = 'https://freddiemac.embs.com/FLoan/secure/login.php'
auth_page_url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'
download_page_url = 'https://freddiemac.embs.com/FLoan/Data/downloadQ.php'

# Define color class for pretty logging
class Colors:
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ITER = '\033[36m'
    ENDC = '\033[0m'

# Define PySpark dataframes schemas
orig_schema = StructType([
    StructField("Credit_Score", DoubleType(), True),
    StructField("First_Payment_Month", StringType(), True),
    StructField("First_Time_Home_Buyer_Flag", StringType(), True),
    StructField("Maturity_Date", StringType(), True),
    StructField("Metropolitan_Area", StringType(), True),
    StructField("MI_Percentage", DoubleType(), True),
    StructField("Num_Units", DoubleType(), True),
    StructField("Occupancy_Status", StringType(), True),
    StructField("CLTV", DoubleType(), True),
    StructField("DTI", DoubleType(), True),
    StructField("UPB", DoubleType(), True),
    StructField("LTV", DoubleType(), True),
    StructField("Int_Rate", DoubleType(), True),
    StructField("Channel", StringType(), True),
    StructField("Prepay_Penalty", StringType(), True),
    StructField("Amortization_Type", StringType(), True),
    StructField("Property_State", StringType(), True),
    StructField("Property_Type", StringType(), True),
    StructField("ZIP_Code", StringType(), True),
    StructField("Loan_Num", StringType(), True),
    StructField("Loan_Purpose", StringType(), True),
    StructField("Loan_Term", DoubleType(), True),
    StructField("Num_Borrowers", DoubleType(), True),
    StructField("Seller_Name", StringType(), True),
    StructField("Servicer_Name", StringType(), True),
    StructField("Sup_Conf_Flag", StringType(), True),
    StructField("Pre_Ref_Loan_Num", StringType(), True),
    StructField("Program_Indicator", StringType(), True),
    StructField("Ref_Indicator", StringType(), True),
    StructField("Prop_Val_Method", DoubleType(), True),
    StructField("Int_Only_Indicator", StringType(), True),
    StructField("MI_Canc_Indicator", StringType(), True)
])

perf_schema = StructType([
    StructField("Loan_Num", StringType(), True),
    StructField("Month_Reporting", StringType(), True),
    StructField("Current_Actual_UPB", DoubleType(), True),
    StructField("Current_Delinquency_Status", StringType(), True),
    StructField("Loan_Age", DoubleType(), True),
    StructField("Remaining_Months_To_Legal_Maturity", DoubleType(), True),
    StructField("Defect_Settlement_Date", StringType(), True),
    StructField("Modification_Flag", StringType(), True),
    StructField("Zero_Balance_Code", StringType(), True),
    StructField("Zero_Balance_Effective_Date", StringType(), True),
    StructField("Current_Int_Rate", DoubleType(), True),
    StructField("Current_Non_Int_UPB", DoubleType(), True),
    StructField("DDLPI", StringType(), True),
    StructField("MI_Recoveries", DoubleType(), True),
    StructField("Net_Sale_Proceeds", DoubleType(), True),
    StructField("Non_MI_Recoveries", DoubleType(), True),
    StructField("Total_Expenses", DoubleType(), True),
    StructField("Legal_Costs", DoubleType(), True),
    StructField("Maintenance_And_Preservation_Costs", DoubleType(), True),
    StructField("Taxes_And_Insurance", DoubleType(), True),
    StructField("Miscellaneous_Expenses", DoubleType(), True),
    StructField("Actual_Loss", DoubleType(), True),
    StructField("Cum_Modification_Cost", DoubleType(), True),
    StructField("Step_Modification_Flag", StringType(), True),
    StructField("Payment_Deferral", StringType(), True),
    StructField("ELTV", DoubleType(), True),
    StructField("Zero_Bal_Removal_UPB", DoubleType(), True),
    StructField("Delinquent_Accrued_Int", DoubleType(), True),
    StructField("Disaster_Delinq_Flag", StringType(), True),
    StructField("Borrower_Assistance_Status_Code", StringType(), True),
    StructField("Curr_Month_Modification_Cost", DoubleType(), True),
    StructField("Curr_Int_UPB", DoubleType(), True)
])


def logging(message):
    """
    Log a message with the current time to the second.
    
    Parameters:
        - message (str): The message to log.
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} - {message}")


def clear_iceberg_table(output_table,
                        year,
                        quarter):
    """
    Clear Iceberg table for the given year and quarter.

    Parameters:
        - output_table (str): Iceberg table name.
        - year (int): data reference year.
        - quarter (int): data reference quarter.
    """

    query = f"""DELETE FROM {output_table}
                WHERE year = {year} AND quarter = {quarter}
                """
    spark.sql(query)


def write_to_iceberg(df, 
                     output_table, 
                     run_date,
                     year,
                     quarter,
                     is_origination,
                     chunk_num):
    '''
    Write data to Iceberg table.

    Parameters:
        - df (pyspark.sql.DataFrame): Spark DataFrame.
        - output_table (str): Iceberg table name.
        - run_date (str): Date of processing.
        - year (int): data reference year.
        - quarter (int): data reference quarter.
        - is_origination (bool): True if origination data, False if performance data.
        - chunk_num (int): Chunk number.
    '''
    try:
        # Add year and quarter columns
        df = df.withColumn('year', lit(year))
        df = df.withColumn('quarter', lit(quarter))

        # Add processing date
        date = datetime.strptime(run_date, "%Y-%m-%d").date()
        df = df.withColumn('date', lit(date))

        # Write to Iceberg
        df.writeTo(output_table).using("iceberg").append()

    except Exception as e:
        raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: failed to write chunk n. {chunk_num} {'origination' if is_origination else 'performance'} for {year}Q{quarter}. Error: {e}")
    else:
        logging(f"{Colors.OKGREEN}SUCCESS{Colors.ENDC}: wrote chunk n. {chunk_num} {'origination' if is_origination else 'performance'} for {year}Q{quarter}")


def process_data(zip_file,
                 file_name,
                 output_table,
                 schema,
                 run_date,
                 year,
                 quarter,
                 is_origination,
                 chunk_size = 1000000):
    '''
    Clear Iceberg table for current year and quarter.
    Open file content in memory in chunks.
    Convert into PySpark dataframes and write to Iceberg.

    Parameters:
        - zip_file (zipfile.ZipFile): Zip file object
        - file_name (str): Source file name.
        - output_table (str): Iceberg table name.
        - schema (pyspark.sql.types.StructType): PySpark schema.
        - run_date (str): Date of processing.
        - year (int): data reference year.
        - quarter (int): data reference quarter.
        - is_origination (bool): True if origination data, False if performance data.
        - chunk_size: Size of each chunk
    '''

    # Process origination data
    try:
        # Clear Iceberg table for current year and quarter
        clear_iceberg_table(output_table, year, quarter)

        # Open the target file within the zipped folder
        with zip_file.open(file_name) as file:

            logging(f"{Colors.OKGREEN}DOWLOADED{Colors.ENDC} {'ORIGINATION' if is_origination else 'PERFORMANCE'} DATA for {year}Q{quarter}")

            # Process data in chunks
            chunk_num = 0
            while True:
                # Read the next chunk_size lines
                lines = list(itertools.islice(file, chunk_size))
                logging(f"{Colors.OKGREEN}FILE SIZE{Colors.ENDC}: {len(lines)} rows")
                logging(f"{Colors.OKGREEN}First lines{Colors.ENDC}: {lines[:5]}")
                if not lines:
                    break

                # Decode lines and strip whitespace
                lines = [line.decode('utf-8').strip() for line in lines]

                chunk_num += 1
                rdd = spark.sparkContext.parallelize(lines)
                df = spark.read.csv(rdd, sep='|', schema=schema, header=False)

                if df is not None and df.count() > 0:
                    # Write to Iceberg table
                    write_to_iceberg(df, 
                                     output_table, 
                                     run_date,
                                     year, 
                                     quarter,
                                     is_origination=is_origination,
                                     chunk_num=chunk_num)
                    # Unpersist the DataFrame to free memory
                    df.unpersist()
                    # Explicitly call the garbage collector
                    gc.collect()
            return
    except Exception as e:
        raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: failed to process {'ORIGINATION' if is_origination else 'PERFORMANCE'} data for {year}Q{quarter}. Error: {e}")


def get_login_response(username,
                       password):
    '''
    Login into Freddie Mac website, accept terms and conditions.
    Return session object and response content.

    Parameters:
        - username (str): Freddie Mac username.
        - password (str): Freddie Mac password.
    '''

    try:
        # Initialize requests session.
        with requests.Session() as sess:
            # Get response from login page.
            response = sess.get(login_page_url)
            # Check if login page loaded successfully
            if response.status_code != 200:
                raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: failed to load login page. Status code: {response.status_code}")

            # Define login payload
            login_payload = {
                'username': username,
                'password': password,
                'pagename': '../menu'
            }
            headers = {
                'User-Agent': 'Chrome/58.0.3029.110'
            }
            # Post login payload to login page
            response_login = sess.post(auth_page_url, data=login_payload, headers=headers)
            logging(f"Login response status code: {response_login.status_code}")
            # Check if login was successful
            if response_login.status_code == 200 and "Please log in" not in response_login.text:
                logging(f"{Colors.OKGREEN}SUCCESS{Colors.ENDC}: Login into freddiemac.embs.com successful")
                # Accept terms and conditions
                download_page_payload = {'accept': 'Yes', 'action': 'acceptTandC', 'acceptSubmit': 'Continue'}
                response_download = sess.post(download_page_url, data=download_page_payload, headers=headers)
                # Check if terms and conditions were accepted
                if response_download.status_code == 200:
                    logging(f"{Colors.OKGREEN}SUCCESS{Colors.ENDC}: Terms and conditions accepted")
                    # Retun session object
                    return sess, response_download.content
                else:
                    raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to accept terms and conditions. Status code: {response_download.status_code}")
            else:
                raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to login. Status code: {response_login.status_code} \n{response_login.text}")   
    except requests.RequestException as e:
        raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: An error occurred during the login process. Error: {e}")
    return None, None


def download_with_retry(session, 
                        url, 
                        max_retries=3, 
                        backoff_factor=1):
    """
    Download file with retry logic and content validation
    
    Parameters:
        - session (requests.Session): requests Session object
        - url (str): URL to download from
        - max_retries (int): Maximum number of retry attempts
        - backoff_factor (int): Backoff factor for retry delays
    """
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504, 429]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    for attempt in range(max_retries):
        try:
            # Make request
            response = session.get(url)
            response.raise_for_status()
            
            # Get content
            content = response.content
                
            # Verify zip file signature
            if not content.startswith(b'PK\x03\x04'):
                raise zipfile.BadZipFile(f"File signature does not match zip format: {content[:10]}")
            
            # Try opening as zip to validate
            try:
                with io.BytesIO(content) as buf:
                    with zipfile.ZipFile(buf) as zf:
                        # Verify file listing
                        files = zf.namelist()
                        if not files:
                            raise ValueError("Empty zip file")
                        else:
                            logging(f"Found {len(files)} files in zip archive: {files}")
            except zipfile.BadZipFile as e:
                raise ValueError(f"Invalid or corrupted zip file. Error: {e}")

            return content
            
        except Exception as e:
            # Log error and retry
            logging(f"{Colors.WARNING}WARNING{Colors.ENDC}: Download attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise RuntimeError(f"Failed to download after {max_retries} attempts")


def start_execution(session,
                    origination_table,
                    performance_table, 
                    run_date, 
                    year, 
                    quarter,
                    orig_schema,
                    perf_schema):
    '''
    Download data and keep them in memory.
    Process ORIGINATION and PERFORMANCE data.

    Inputs:
        - session (requests.Session): Session object.
        - origination_table (str): Iceberg table name.
        - performance_table (str): Iceberg table name.
        - run_date (str): Date of processing.
        - year (int): data reference year.
        - quarter (int): data reference quarter.
        - orig_schema (pyspark.sql.types.StructType): PySpark schema for origination data.
        - perf_schema (pyspark.sql.types.StructType): PySpark schema for performance data.
    '''

    # Download data folder
    url = download_page_url + '?f=historical_data_' + str(year) + 'Q' + str(quarter)

    if session is None:
        raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to establish a session.")

    # Try to download the file
    try:
        # Download with validation and retry logic
        content = download_with_retry(session, url)
        
        # Process the downloaded content
        with io.BytesIO(content) as buffer:
            # Unzip the buffer content
            with zipfile.ZipFile(buffer, 'r') as zip_file:
                # Define orgination data file name
                orig_file_name = f'historical_data_{year}Q{quarter}.txt'
                # Define performance data file name
                perf_file_name = f'historical_data_time_{year}Q{quarter}.txt'
                
                # Processing ORIGINATION data
                process_data(zip_file,
                                orig_file_name,
                                origination_table,
                                orig_schema,
                                run_date,
                                year,
                                quarter,
                                is_origination=True)
                # Processing PERFORMANCE data
                process_data(zip_file,
                                perf_file_name,
                                performance_table,
                                perf_schema,
                                run_date,
                                year,
                                quarter,
                                is_origination=False)

    except Exception as e:
        raise RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to process data: {str(e)} \nContent: {content[:100]}")



if __name__ == "__main__":
    
    # Get login session
    session, response_content = get_login_response(FM_USERNAME, FM_PASSWORD)

    # Iterate over all available quarters for the given year
    for quarter in range(1, last_quarter+1):
            
        logging(f"{Colors.OKGREEN}START Processing DATA for {year}Q{quarter}{Colors.ENDC}")
        start_execution(session,
                        origination_table,
                        performance_table, 
                        run_date, 
                        year, 
                        quarter,
                        orig_schema,
                        perf_schema)
        logging(f"{Colors.OKGREEN}END Processing DATA for {year}Q{quarter}{Colors.ENDC}")

    job.commit()