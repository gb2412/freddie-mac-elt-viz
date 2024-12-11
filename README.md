# Freddie Mac Dataset ELT and Dashboard 
![Untitled_design-removebg-preview](https://github.com/user-attachments/assets/d2f139fc-7f37-4f7b-9f76-c42e931e91bf)
An end-to-end ELT pipeline to download, load into a database and transform into key metrics [Freddie Mac Single Family Loan-Level Dataset](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset).

#### ©️ Author: [Giulio Bellini](https://github.com/gb2412)

## Table of Contents
- [Overview](#overview)
	- [🔥 Project Motivation](#-project-motivation)
	- [🎯 Target Audience](#-target-audience)
   	- [🔩 Components](#-components)
   	- [🔧 Tech Stack](#-tech-stack)
- [📊 Dashboard](#-dashboard)
- [💾 Data](#-data)
	- [📆 Challenge 1: No fixed release calendar](#-challenge-1-no-fixed-release-calendar)
	- [💿 Challenge 2: Data as zipped text files](#-challenge-2-data-as-zipped-text-files)
	- [🔄 Challenge 3: Corrections and updates](#-challenge-3-corrections-and-updates)
- [⛽ ELT pipeline](#-elt-pipeline)
  	- [👉 pre-ELT tasks](#-pre-elt-tasks)
  	- [👉 EL tasks](#-el-tasks)
  	  	- [Write](#write)
  	  	- [Audit](#audit)
  	  	- [Publish](#publish)
  	- [👉 T tasks](#-t-tasks)
- [🏭 Data Modelling](#-data-modelling)
	- [👉 Staging](#-staging)
 	- [👉 Intermediate](#-intermediate)
  	- [👉 Metrics](#-metrics)
- [📈 Next Steps](#-next-steps)


## Overview

### 🔥 Project Motivation
My background is in economics and finance. I'm fascinated by quantitative risk management, particularly credit risk modelling. Accurately predicting how many and which borrowers will default allows banks and lending companies to make important strategic decisions. Needless to say, machine learning is a natural fit in this space. But beyond fancy neural networks and tree-based models, analytics remains critical to risk management. In this project, which marks the end of the [Dataexpert](https://www.dataexpert.io) **Analytics Engineering Bootcamp** V1, I decided to focus on analytics and discover the complexity and beauty behind simple time series and percentages.

### 🎯 Target Audience
This project will benefit anyone who needs to interact with the dataset and is looking for a programmatic way to do so. The pipeline can be used by credit model developers or academic researchers, while the dashboard provides insights for mortgage industry practitioners.

### 🔩 Components
The project has two main components: an [ELT pipeline](#-elt-pipeline) and a [dashboard](#-dashboard). The ELT pipeline extracts mortgage origination and monthly performance data from Freddie Mac's website, loads it into a data lake, and transforms it into metrics tables. The dashboard displays the time series of these key metrics, providing a comprehensive, up-to-date view of the size and performance of Freddie Mac's mortgage portfolio over time.

### 🔧 Tech Stack
- [Astronomer](https://www.astronomer.io/)-[Airflow](https://airflow.apache.org/) for orchestration
- [Starburst](https://www.starburst.io/)-[Trino](https://trino.io/) for querying
- [AWS Glue](https://aws.amazon.com/glue/) and [S3](https://aws.amazon.com/s3/) with [Tabular](https://www.tabular.io/) and [Iceberg](https://iceberg.apache.org/) for storing
- [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) for loading
- [dbt](https://www.getdbt.com/) for transformation
- [Snowflake](https://www.snowflake.com/en/emea/) for storing metrics tables
- [Docker](https://www.docker.com/) for containerization
- [Grafana](https://grafana.com/grafana/dashboards/) for visualization


## 📊 Dashboard

I created the dashboard in **Grafana** in the name of simplicity and elegance. In one screen, you get a complete view of the size and performance of the portfolio, as well as their evolution over time. 

[link to dashboard](https://giuliobellini.grafana.net/public-dashboards/efa53b6b222843ec84671f37d879c02c)

![demo](https://github.com/user-attachments/assets/5c20cbe7-2891-4d24-8e67-7264fe1ef32b)


The time series in **Mortgage Portfolio Size** represent the total dollar amount of loans and the number of loans in the portfolio.

**Monthly Profit & Loss** shows the total amount of interest paid each month and the monthly losses on defaulted loans. The losses arise from having to sell the loans at a price lower than the purchase price, from having to sell the property securing the mortgage at a price lower than the unpaid principal balance, or from having to hold the property while waiting to sell it.

Finally, **Portfolio Default Rate** shows the one-year default rate, i.e. the average probability of default of the mortgages in the portfolio over the 12 months following the observation date. As this rate is not known at the observation date, it has to be estimated. The graph shows the estimates of a machine learning model I developed, a Sparse Generalised Additive Model. For comparison, the actual default rate is also plotted to check the accuracy of the model. For the last 12 observation dates, the actual default rate is not available because the 12 months necessary to calculate it have not yet elapsed.


## 💾 Data
[Freddie Mac's Single-Family Loan-Level Dataset](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset) is probably the largest free source of mortgage loan-level data available online. [Freddie Mac](https://www.freddiemac.com/) (FM) is a US government-sponsored enterprise. It buys mortgages from lenders and either holds them in its portfolio or packages the loans into mortgage-backed securities (MBS) that can be sold to investors. FM has published loan-level data (origination and monthly performance) for all mortgages it has purchased since 1999. The data is updated quarterly with a six-month lag.

Although these data are an excellent resource for credit risk modelling and analytics, there are three main challenges that make them cumbersome to use:
1.  Data are not published on a precise [calendar](https://www.freddiemac.com/fmac-resources/research/pdf/release_notes.pdf).
2. There are no APIs or database to query, the only way to access the data is to log on to the [website](https://freddiemac.embs.com/FLoan/secure/login.php?pagename=downloadQ) and download it as zipped folders of text files. Each year quarter (e.g. 2015Q1) corresponds to a folder containing a file of origination data for the mortgages purchased in that quarter and a file of performance data for the same mortgages. The performance file is updated with each data release.
3. With each release, FM may publish corrections and updates to the historical data in the dataset without specifying which records have been affected.

In this project, I addressed each of these challenges to make the interaction with the dataset more seamless. 

Here is how...

### 📆 Challenge 1: No fixed release calendar
My DAG runs daily and checks if a new quarter of data has been released. If so, the DAG continues, otherwise all downstream loading and transformation processes are skipped. This ensures that the production tables and dashboard are always up to date, with a maximum delay of 24 hours.

### 💿 Challenge 2: Data as zipped text files
The glue_iceberg_load.py script extracts and loads the data programmatically. It accesses the FM website, logs in with username and password, accepts the terms and conditions and downloads the data from the download page. The data is downloaded one quarter at a time, held in memory, unzipped, converted into a list and finally into a [Spark](https://spark.apache.org/) dataframe to be appended to their respective Iceberg table. The script is executed by submitting an AWS Glue job via `boto3`. See [below](#-el-tasks) for more details on the EL process.

### 🔄 Challenge 3: Corrections and updates
There is no information about which records have been changed, and the data format makes it impossible to implement change data capture without downloading the new data. For these reasons, each time a new quarter is released, a backfill DAG is triggered to extract, load and transform the data in its current version, replacing the previous one. This approach ensures that the data in the database reflects any change/correction to the source, although it is time-consuming and computationally expensive. Users have the option to backfill only the years they are interested in updating.


## ⛽ ELT pipeline

![Screenshot 2024-12-11 131610](https://github.com/user-attachments/assets/e1cc3587-992d-4685-9ba4-c46a9221a77e)

The bulk of this project is the ELT pipeline, developed as an **Airflow DAG**. The DAG can be decomposed into three main components or groups of tasks: the [pre-ELT](#-pre-elt-tasks), the [EL](#-el-tasks) and the [T](#-t-tasks) tasks.

### 👉 pre-ELT tasks
![Screenshot 2024-12-05 150932](https://github.com/user-attachments/assets/20cd5395-4474-4588-bfce-1030ecfcfc78)


As introduced by [Challenge 1](#-challenge-1-no-fixed-release-calendar) above, FM does not publish new data according to a predefined schedule. The initial DAG tasks run every day and are designed to assess whether new data has been released and whether to start the backfilling process or skip all downstream tasks. 
1. `start_execution` is a `DummyOperator` that marks the start of the DAG.
2. `get_year_quarter_release` logs into Freddie Mac's dataset website and extracts the most recent available year-quarter.
3. `create_prod_tables` is a `TaskGroup` that creates the production tables if they do not already exist in the database, thus preventing downstream tasks from breaking.
4. `get_year_quarter_table` extracts the most recent year-quarter from the production tables by running a Trino query through the `run_trino_query()` function.
5. `check_for_release` compares the two quarters and determines that new data has been released if the year-quarter from the website is more recent than the year-quarter from the tables.
6. `new_data_released` is a `ShortCircuitOperator` which, if `check_for_release` returns a falsy output, skips all downstream tasks.


### 👉 EL tasks

![Screenshot 2024-12-05 151022](https://github.com/user-attachments/assets/a4975f77-091b-4c8d-9f74-3197497485cf)


When new data is released, the backfill process is invoked as described in [Challenge 3](#-challenge-3-corrections-and-updates). 
1. `get_years_quarters_list` defines the list of all years and quarters available for download. 
2. `create_clear_audit_origination` and `create_clear_audit_performance` `TaskGroup`s create and clear the audit tables to implement the **WAP** (write-audit-publish) process described below.

#### Write
`load_to_audit_tables` is the actual EL task. It downloads the data and loads it into the Iceberg tables in AWS. It is a dynamic task: a separate glue job is initiated for each year. The glue job executes the `glue_iceberg_load.py` script and is submitted using the `create_and_run_glue_job` function from `glue_job_runner.py`, which is a wrapper around the `create_glue_job` function from `glue_job_submission.py`.

In each job, the corresponding year's data is processed one quarter at a time, performing the following steps:
1. downloaded with `requests`
2. held in memory with `io.BytesIO()`
3. unzipped with `zipfile.ZipFile()`
4. read in chunks of 1 million lines with `itertools.islice()`
5. chunk stored as a `list`
6. chunk converted to a PySpark dataframe
7. chunk appended to the respective audit Iceberg table

Creating a separate glue job for each year allows the EL process to be repeated for specific years in the event of runtime errors or data quality fails. The data for each year is processed in batches of 1 million rows at a time to avoid OOM errors. The batch size can be tuned according to the resources available to achieve the optimum trade-off between cost and computation time.

The most error-prone step is downloading the data. Due to the size of the files, it is not uncommon for them to be corrupted during the download process and therefore unable to be unzipped. A retry strategy has been implemented to handle this type of error.

#### Audit
After loading the data into the audit tables, data quality checks are performed. 
These ensure that:
- there are NOT fewer unique loans in the audit origination table than in the production origination table.
- for each observation date (`month_reporting`), there are NOT fewer unique loans in the audit performance table than in the production performance table.
These tests are designed to ensure no records are removed from the production tables when the pipeline is run. The tests are run on a per-year basis so that if they fail, only the corresponding Glue jobs can be re-run. Column-level quality tests are carried out in the transformation phase.

#### Publish
If the data quality tests are successful, the data is transferred from the audit tables to the production tables, replacing its previous version.


### 👉 T tasks

![Screenshot 2024-12-06 165358](https://github.com/user-attachments/assets/4560e738-bd09-4da8-a8cc-485bd45b38f2)


All transformations from the raw data tables to the metrics tables were implemented in **dbt**. The data lineage diagram below shows the four transformation phases, represented by four groups of tables: **sources**, **staging**, **intermediate** and **metrics**. A detailed description of each of the tables/dbt-models and their respective data tests follows. 

![SOURCE (3) 1](https://github.com/user-attachments/assets/d7dfb0e6-c4ad-4529-8a92-ef78608a576b)


The metrics tables are then loaded into **Snowflake** to be easily accessible from **Grafana** to power the dashboard. Finally, the `end_execution` `DummyOperator` marks the end of the DAG.


## 🏭 Data Modelling
My data model is based on two **source tables**: `raw_mortgage_origination` and `raw_mortgage_perfromance`, which contain the origination and performance data for all loans in the portfolio. They are the output of the **WAP-EL** process and can therefore be considered the single source of truth for all downstream tables. **Staging** and **intermediate** tables are built on top of them, leading to the calculation of the **aggregated metrics** shown in the dashboard.

| Table                    | Rows                     | Description                                                   | Partitioned by                |
| ------------------------ | ------------------------ | ------------------------------------------------------------- |-------------------------------|
| raw_mortgage_origination | 18355264 (18 million)   | Origination information on all Freddie Mac mortgages.         | `year`, `first_payment_month`  |
| raw_mortgage_Performance | 774702357 (774 million) | Monthly performance information on all Freddie Mac mortgages. | `year`, `month_reporting`      |

### 👉 Staging
#### stg__raw_origination
A one-to-one mapping to the `raw_mortgage_origination` table with proper encoding for `NULL` values and enums. 

| Column Name                | Data Type | Description                                                 |
| -------------------------- | --------- | ----------------------------------------------------------- |
| loan_num                   | VARCHAR   | Unique identifier for each loan.                            |
| first_payment_month        | DATE      | The month of the first payment for the loan.                |
| year                       | INT       | The year the loan was originated.                           |
| quarter                    | INT       | The quarter the loan was originated.                        |
| loan_term                  | INT       | The term of the loan in months.                             |
| maturity_date              | VARCHAR   | The date when the loan matures.                             |
| amortization_type          | VARCHAR   | The type of amortization for the loan.                      |
| int_rate                   | DOUBLE    | The interest rate of the loan.                              |
| loan_purpose               | VARCHAR   | The purpose of the loan (e.g., purchase, refinance).        |
| credit_score               | INT       | The credit score of the borrower (300-850).                 |
| upb                        | BIGINT    | The unpaid principal balance of the loan.                   |
| ltv                        | DOUBLE    | The loan-to-value ratio of the loan.                        |
| cltv                       | DOUBLE    | The combined loan-to-value ratio of the loan.               |
| dti                        | DOUBLE    | The debt-to-income ratio of the borrower.                   |
| first_time_home_buyer_flag | VARCHAR   | Indicates if the borrower is a first-time home buyer.       |
| zip_code                   | VARCHAR   | The three-digit ZIP code of the property securing the loan. |
| metropolitan_area          | VARCHAR   | The metropolitan area of the property securing the loan.    |
| property_state             | VARCHAR   | The state where the property securing the loan is located.  |
| property_type              | VARCHAR   | The type of property securing the loan.                     |
| occupancy_status           | VARCHAR   | The occupancy status of the property securing the loan.     |
| num_units                  | INT       | The number of units in the property securing the loan.      |
| prop_val_method            | INT       | The method used to value the property securing the loan.    |
| num_borrowers              | INT       | The number of borrowers on the loan.                        |
| channel                    | VARCHAR   | The origination channel of the loan.                        |
| seller_name                | VARCHAR   | The name of the seller of the loan.                         |
| servicer_name              | VARCHAR   | The name of the servicer of the loan.                       |
| mi_percentage              | DOUBLE    | The mortgage insurance percentage of the loan.              |
| mi_canc_indicator          | VARCHAR   | Indicates if the mortgage insurance was canceled.           |
| prepay_penalty             | VARCHAR   | Indicates if there is a prepayment penalty on the loan.     |
| int_only_indicator         | VARCHAR   | Indicates if the loan is interest-only.                     |
| sup_conf_flag              | VARCHAR   | Indicates if the loan is a super conforming loan.           |
| program_indicator          | VARCHAR   | The program indicator for the loan.                         |
| ref_indicator              | VARCHAR   | The relief refinance indicator for the loan.                |
| pre_ref_loan_num           | VARCHAR   | The pre-relief refinance loan number.                       |
| date                       | DATE      | The date the loan data was loaded.                          |

Data tests:
- `loan_num` is UNIQUE and NOT `NULL`
- `int_rate` is POSITIVE
- `credit_score` is POSITIVE
- `upb` is POSITIVE


#### stg__raw_performance
A one-to-one mapping to the `raw_mortgage_performance` table with proper encoding for `NULL` values and enums. 

| Column Name                        | Data Type | Description                                                                         |
| ---------------------------------- | --------- | ----------------------------------------------------------------------------------- |
| loan_num                           | VARCHAR   | Unique identifier for each loan.                                                    |
| month_reporting                    | DATE      | The month the performance data was reported.                                        |
| year                               | INT       | The year the loan was originated.                                                   |
| quarter                            | INT       | The year the loan was originated.                                                   |
| remaining_months_to_legal_maturity | DOUBLE    | The number of months remaining until the loan matures.                              |
| loan_age                           | DOUBLE    | The age of the loan in months.                                                      |
| current_actual_upb                 | DOUBLE    | The current unpaid principal balance of the loan.                                   |
| current_delinquency_status         | VARCHAR   | The current delinquency status of the loan.                                         |
| ddlpi                              | VARCHAR   | Due date of last paid installment.                                                  |
| current_int_rate                   | DOUBLE    | The current interest rate of the loan.                                              |
| eltv                               | DOUBLE    | The estimated current loan-to-value ratio of the loan.                              |
| curr_int_upb                       | DOUBLE    | The current interest bearing unpaid principal balance.                              |
| current_non_int_upb                | DOUBLE    | The current non-interest bearing unpaid principal balance.                          |
| modification_flag                  | VARCHAR   | Indicates if the loan has been modified in current or prior period.                 |
| step_modification_flag             | VARCHAR   | Indicates if the loan has a step modification.                                      |
| curr_month_modification_cost       | DOUBLE    | The modification cost for the current month.                                        |
| cum_modification_cost              | DOUBLE    | The cumulative modification cost.                                                   |
| borrower_assistance_status_code    | VARCHAR   | The assistance plan the borrower is enrolled in.                                    |
| payment_deferral                   | VARCHAR   | Indicates if the payment has been deferred.                                         |
| disaster_delinq_flag               | VARCHAR   | Indicates if the loan is delinquent due to a disaster.                              |
| zero_balance_code                  | VARCHAR   | The code indicating the reason for a zero balance.                                  |
| zero_balance_effective_date        | VARCHAR   | The effective date of the zero balance.                                             |
| defect_settlement_date             | VARCHAR   | The date of the defect settlement.                                                  |
| zero_bal_removal_upb               | DOUBLE    | The unpaid principal balance at the time of zero balance removal.                   |
| delinquent_accrued_int             | DOUBLE    | The delinquent accrued interest owned at default.                                   |
| actual_loss                        | DOUBLE    | The actual loss on the loan.                                                        |
| mi_recoveries                      | DOUBLE    | The mortgage insurance recoveries.                                                  |
| non_mi_recoveries                  | DOUBLE    | The non-mortgage insurance recoveries.                                              |
| net_sale_proceeds                  | DOUBLE    | The net proceeds from the sale of the property.                                     |
| total_expenses                     | DOUBLE    | The total expenses related to acquiring, maintaining and/or disposing the property. |
| legal_costs                        | DOUBLE    | The legal costs associated with the sale of the property.                           |
| maintenance_and_preservation_costs | DOUBLE    | The maintenance and preservation costs associated with the sale of the property.    |
| taxes_and_insurance                | DOUBLE    | The taxes and insurance associated with the sale of the property.                   |
| miscellaneous_expenses             | DOUBLE    | The miscellaneous expenses associated with the sale of the property.                |
| date                               | DATE      | The date the performance data was loaded.                                           |

Data tests:
- `loan_num` is NOT `NULL`
- `month_reporting` is NOT `NULL`
- `int_rate` is POSITIVE
- `credit_score` is POSITIVE
- `upb` is POSITIVE

### 👉 Intermediate
#### int__actual_defaults
This table shows for each loan in each reference month its current default status (`curr_default`) and whether it will default at any time in the next 12 months (`ever_bad_one_year_default`). 
The default definition used is:
- 90+ days past due
	or 
- Any of the following occurrences: third parties, short sales, write-offs, REO sales, whole loans.
The `ever_bad_one_year_default` column is calculated using a 12-month window function and is `NULL` for the last twelve months.

| Column Name               | Data Type | Description                                       |
| ------------------------- | --------- | ------------------------------------------------- |
| loan_num                  | VARCHAR   | Unique identifier for each loan.                  |
| month_reporting           | DATE      | The month the performance data was reported.      |
| curr_default              | INT       | The current default status of the loan.           |
| ever_bad_one_year_default | INT       | The ever-bad one-year default status of the loan. |

Data tests:
- `loan_num` is NOT `NULL`
- `curr_default` accepted vales: 0 or 1
- `ever_bad_one_year_default` accepted vales: 0, 1 or `NULL`
To test the logic of the window function in `ever_bad_one_year_default`, a unit test has been implemented.


#### int__ml_model_predictions
This table shows the 12-month ever-bad probability of default for each loan at each reporting month. The probabilities are computed by a **machine learning** model trained on Freddie Mac data as part of a [separate project](https://github.com/gb2412/MSc_Thesis/blob/main/Interpretable%20Machine%20Learning%20in%20Credit%20Risk%20Modelling.pdf). The model is a **sparse Generalised Additive Model** with only 14 binary features. As the model is fully transparent, it was implemented directly in the SQL query by computing the features and hardcoding the coefficients. This is not standard practice for deploying ML models, but in this case it is enough for inference. 

| Column Name     | Data Type | Description                                   |
| --------------- | --------- | --------------------------------------------- |
| loan_num        | VARCHAR   | Unique identifier for each loan.              |
| month_reporting | VARCHAR   | The month the performance data was reported.  |
| prediction      | DOUBLE    | Probability of default from sparse GAM model. |

Data tests:
- `loan_num` is NOT `NULL`
- `prediction` is a VALID PROBABILITY, in the range [0,1]

  
### 👉 Metrics
#### m__principal
This table stores the total unpaid principal balance at each reporting month, i.e. the amount of debt outstanding on all mortgages in the portfolio at that time.

| Column Name       | Data Type | Description                                                   |
| ----------------- | --------- | ------------------------------------------------------------- |
| month_reporting   | VARCHAR   | The month the performance data was reported.                  |
| total_current_upb | DOUBLE    | The total unpaid principal balance on all loans in the month. |

Data tests:
- `month_reporting` is NOT `NULL`
- `total_current_upb` is NOT `NULL` and POSITIVE

  
#### m__loans_number
This table shows the number of distinct loans in the portfolio for each reporting month.

| Column Name     | Data Type | Description                                  |
| --------------- | --------- | -------------------------------------------- |
| month_reporting | VARCHAR   | The month the performance data was reported. |
| total_loans     | BIGINT    | The number of loans in the portfolio.        |

Data tests:
- `month_reporting` is NOT `NULL`
- `total_loans` is NOT `NULL` and POSITIVE

  
#### m__interest
This table shows the total interest paid each month on all mortgages. Interest due but not paid is not taken into account. If a delinquent borrower pays installments in arrears, it is assumed that he pays the oldest installment first.

| Column Name            | Data Type | Description                                        |
| ---------------------- | --------- | -------------------------------------------------- |
| month_reporting        | VARCHAR   | The month the performance data was reported.       |
| total_monthly_interest | DOUBLE    | The total interest paid on all loans in the month. |

Data tests:
- `month_reporting` is NOT `NULL`
- `total_monthly_interest` is NOT `NULL` and NON-NEGATIVE

#### m__loss
This table shows the losses incurred for each month. The losses are caused by the default of the mortgages and the subsequent foreclosure and sale of the property. Unpaid interest, being unrealized losses, is not taken into account until an actual default event occurs.

| Column Name        | Data Type | Description                                                  |
| ------------------ | --------- | ------------------------------------------------------------ |
| month_reporting    | VARCHAR   | The month the performance data was reported.                 |
| total_monthly_loss | DOUBLE    | The total loss incurred on all defaulted loans in the month. |

Data tests:
 - `month_reporting` is NOT `NULL`
 - `total_monthly_loss` is NOT `NULL` and NON-POSITIVE


#### m__portfolio_risk
This table shows the current default rate, the actual one-year ever-bad default rate and the average one-year ever-bad default probability of the portfolio as calculated by the machine learning model. 

| Column Name                              | Data Type | Description                                                     |
| ---------------------------------------- | --------- | --------------------------------------------------------------- |
| month_reporting                          | VARCHAR   | The month the performance data was reported.                    |
| current_default_rate                     | DOUBLE    | Average current default rate.                                   |
| ever_bad_one_year_default_rate           | DOUBLE    | Average next-year default rate.                                 |
| predicted_ever_bad_one_year_default_rate | DOUBLE    | Average predicted probability of default from sparse GAM model. |

Data tests:
- `month_reporting` NOT `NULL`
- `current_default_rate` is NOT `NULL` and a VALID PROBABILITY, in the range [0,1]
- `ever_bad_one_year_default_rate` is a VALID PROBABILITY, in the range [0,1]
- `predicted_ever_bad_one_year_default_rate` is a VALID PROBABILITY, in the range [0,1]

## 📈 Next Steps
- Creating additional metrics and visualizations, especially by exploiting demographic data from the origination table.
- Introducing additional data quality checks and unit tests into the pipeline.
- Implementing a proper ML workflow for inference and training of machine learning models on the dataset.
