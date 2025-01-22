import requests
import re
from datetime import datetime

# URLs
login_page_url = 'https://freddiemac.embs.com/FLoan/secure/login.php'
auth_page_url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'
download_page_url = 'https://freddiemac.embs.com/FLoan/Data/downloadQ.php'

# Define color class for logging
class Colors:
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ITER = '\033[36m'
    ENDC = '\033[0m'


def logging(message):
    """
    Log a message with the current time to the second.
    
    Parameters:
        - message (str): The message to log.
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} - {message}")


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
                logging(f"{Colors.FAIL}ERROR{Colors.ENDC}: failed to load login page. Status code: {response.status_code}")
                return None, None
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
                    RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to accept terms and conditions. Status code: {response_download.status_code}")
            else:
                RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: Failed to login. Status code: {response_login.status_code} \n{response_login.text}")   
    except requests.RequestException as e:
        RuntimeError(f"{Colors.FAIL}ERROR{Colors.ENDC}: An error occurred during the login process. Error: {e}")
    return None, None


def get_most_recent_quarter(html_content):
    """
    Retrieve most recente year-quarter from the HTML content.

    Parameters:
        - html_content (str): HTML content.
    """
    # Decode the HTML content if it's in bytes
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8')

    # Find all links that match the pattern for historical data
    pattern = re.compile(r'download.php\?f=historical_data_(\d{4}Q\d)')
    matches = pattern.findall(html_content)

    # Find the most recent quarter with lexicographical comparison
    if matches:
        most_recent_quarter = max(matches)
        return most_recent_quarter
    else:
        return None

def get_year_quarter_release(username,
                             password):
    """
    Log into Freddie Mac website and retrieve the most recent year-quarter.

    Parameters:
        - username (str): Freddie Mac username.
        - password (str): Freddie Mac password.
    """
    # Login into Freddie Mac website
    _, response_content = get_login_response(username, password)

    # Define data cut-off quarter
    try:
        year_quarter = get_most_recent_quarter(response_content)
        current_year = int(year_quarter[:4])
        current_quarter = int(year_quarter[5:])
    except:
        logging(f'{Colors.FAIL}ERROR{Colors.ENDC}: Could not get data cut-off quarter')
        return None, None
    
    return current_year, current_quarter


def check_for_data_release(ti, start_year):
    """
    Check if there is a new data released.
    If yes, return a list of all available year-quarter for download.

    Parameters:
        - ti (airflow.models.TaskInstance): Task instance object.
        - start_year (int): Start year for the data.
    """

    # Pull most recent released year-quarter
    current_year, current_quarter = ti.xcom_pull(task_ids='get_year_quarter_release')
    print(ti.xcom_pull(task_ids='get_year_quarter_release'))
    
    # Pull most recent year-quarter in the table
    try:
        last_year, last_quarter = ti.xcom_pull(task_ids='get_year_quarter_table')[0]
    except:
        last_year, last_quarter = None, None
    print(ti.xcom_pull(task_ids='get_year_quarter_table'))

    # Check if there is a new release
    if current_year and current_quarter:
        # List of year-max_quarter for branching
        year_max_quarter = []
        for year in range(start_year, current_year+1):
            if year == current_year:
                year_max_quarter.append((year, current_quarter))
            else:
                year_max_quarter.append((year, 4))
        # Return list if there is a new release
        if (last_year is None and last_quarter is None) or \
            (current_year > last_year) or \
            (current_year == last_year and current_quarter > last_quarter):
            return year_max_quarter
        else:
            return []
    else:
        return []

