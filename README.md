# Sparkify Data Lake
#### A data lake and dimensional data model hosted on AWS

Sparkify Data Lake is an application which reads song and event data from S3, transforms them into a dimension model then saves the results back to S3 as parquet files.. The dimension model will allow Sparkify (a fictional music streaming app) to slice their event data by dimensions (categories) to discover new insights and trends.

With up-to-date analysis of user behaviour, Sparkify can capitalise on trends as they happen to increase user engagement.

The dimensional model has been designed with accessibility in mind; business users will spend less time preparing the data for analysis and more time gaining insights from it.
___

## Setup Instructions

### Create an IAM User
- Create an AWS account with full access to AWS services.
- Sign in to the [IAM console](https://console.aws.amazon.com/iam/) with your AWS credentials.
- From the navigation panel, select **Users**, then click the **Add user** button.
- Enter a **User name**.
- Enable **Programmatic access** (this is vital for the application to function).
- Click **Next: Permissions** and select **Attach existing policies directly**.
- Enter "iam" in the **Filter policies** field and select **IAMFullAccess** by clicking its checkbox.
- Enter "amazonelasticmap" in the **Filter policies** field and select **AmazonElasticMapReduceFullAccess** by clicking its checkbox.
- Click **Next: Tags**, this application does not make use of tags, so skip this section by clicking **Next: Review**.
- Check that both policies are present and click **Create user**.
- You should see a "Success" message and an option to **Download .csv**. Ensure you download the .csv file and store it securely, it is not possible to retrieve the IAM user credentials beyond this point.

### Security Considerations
To avoid the risk of a data breach, no AWS access credentials are stored in this application's configuration files. Instead, the application references credentials configured via the AWS CLI installed on the user's machine to ensure they are not exposed during operation.

### Install AWS CLI
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
- Check the installation was a success by opening a terminal and running the command `aws --version`. The AWS CLI version number will be displayed if it installed successfully.

### Add AWS Credentials
- Run `aws configure` in the terminal and enter the IAM user access keys from the .csv file downloaded previously. Further instructions can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).

### Update Application Config File
The application automatically creates all AWS infrastructure required for the ETL operation according to the settings stored in `biapp/settings/envs.cfg`.

Open the `envs.cfg` file in a text editor and update the settings as follows:

- **EMR_LOG_URI**: This is the S3 bucket URI the EMR cluster will write log files to, update the value using the following convention: `s3://aws-logs-<UNIQUE BUCKET NAME>-<AWS REGION>/`.
- Example: `s3://aws-logs-123456789123-us-west-2`

- **S3_CODE_BUCKET**: Enter the name of the S3 bucket the application will deploy to. As bucket names are shared by all AWS users, ensure that a unique name is entered here in kebab-case style.
- Example: `my-code-bucket`.

- **S3_DATA_LAKE**: Enter the unique, kebab-case name of the S3 bucket to save the output of the ETL operation to.
- Example: `my-data-lake`

- **S3_OUTPUT_DATA**: This is the S3 URI the results of the ETL operation will be saved to. This should be the bucket name assigned to the `S3_DATA_LAKE` setting with an `s3a://` prefix:
- Example: `s3a://my-data-lake`.

Save the `envs.cfg` file and close.
___


## Operation Instructions

### Running the Application
This application requires Python 3 to run and assumes you have your Python path configured to start Python with `python`. Please amend the suggested commands accordingly to match your setup.

- Open a terminal and navigate to the application directory.
- Install the required Python packages by running `pip install -r requirements.txt`.
- Run `python deploy.py` to deploy the application to AWS, status messages will be logged to the terminal as the program executes.

#### Deployment Process
This application will create the AWS infrastructure required to run the Sparkify ETL process end-to-end:

1. Roles and policies are created to enable cross service access between our EMR cluster (compute) and S3 data lake (storage).
2. S3 buckets are created to store the application code and the result of the ETL operation.
3. An EMR cluster is spun up and the application is copied to the master node's local disk.
4. Jobs are assigned to the EMR cluster and the master node executes the application code.
5. The ETL process reads data from S3, transforms it into a set of dimensional tables and writes the result back to S3.

#### Profiling Queries
As the ETL process writes each table, a profiling query is run to check for duplicate primary keys. The results of these queries are logged to the terminal at runtime and you should expect to see no records returned.
___


## Dimensional Data Model

### Data Distribution
The data distribution in Spark is determined by the `partition` columns which determine how the records are distributed among the EMR clusters and ultimately saved to storage.

#### Fact Table
Since we will be querying recent data frequently to keep up to date with user behaviour, the event timestamp of the `fact_songplays` table is used to partition the data files. This is so that time filtered queries can skip entire blocks of the data that fall outside of the time range, drastically reducing the number of records the query runner needs to scan.

#### Dimension Tables
All dimension tables have been deduplicated by their primary keys and distributed by the dimensions specified in the project specification. Time related tables are distributed by year and month to encourage an even data spread and song data is distributed by year and artist.
___


## Database Schema

#### Table Name: `dim_artists`
- Partition: `None`
- Sort key: `artist_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `artist_id (PK)`    | VARCHAR             | Plain Text          |
| `name`              | VARCHAR             | Plain Text          |
| `location`          | VARCHAR             | Plain Text          |
| `latitude`          | NUMERIC             | Decimal Number      |
| `longitude`         | NUMERIC             | Decimal Number      |

#### Table Name: `dim_songs`
- Partition: `year, artist_name`
- Sort key: `song_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `song_id (PK)`      | VARCHAR             | Plain Text          |
| `artist_id`         | VARCHAR             | Plain Text          |
| `artist_name`       | VARCHAR             | Plain Text          |
| `title`             | VARCHAR             | Plain Text          |
| `year`              | SMALLINT            | Whole Number        |
| `duration`          | NUMERIC             | Decimal Number      |

#### Table Name: `dim_time`
- Partition: `year, month`
- Sort key: `start_time`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `start_time`        | TIMESTAMP           | Date and Time       |
| `hour`              | SMALLINT            | Whole Number        |
| `day`               | SMALLINT            | Whole Number        |
| `weekday`           | SMALLINT            | Whole Number        |
| `week`              | SMALLINT            | Whole Number        |
| `month`             | SMALLINT            | Whole Number        |
| `year`              | SMALLINT            | Whole Number        |
___

#### Table Name: `dim_users`
- Partition: `None`
- Sort key: `user_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `user_id (PK)`      | BIGINT              | Whole Number        |
| `first_name`        | VARCHAR             | Plain Text          |
| `last_name`         | VARCHAR             | Plain Text          |
| `gender`            | VARCHAR             | Plain Text          |
| `level`             | VARCHAR             | Plain Text          |
___

#### Table Name: `fact_songplays`
- Partition: `year, month`
- Sort key: `songplay_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `songplay_id (PK)`  | BIGINT              | Whole Number        |
| `start_time`        | TIMESTAMP           | Date and Time       |
| `user_id`           | BIGINT              | Whole Number        |
| `level`             | VARCHAR             | Plain Text          |
| `song_id`           | VARCHAR             | Plain Text          |
| `artist_id`         | VARCHAR             | Plain Text          |
| `session_id`        | BIGINT              | Whole Number        |
| `location`          | VARCHAR             | Plain Text          |
| `user_agent`        | VARCHAR             | Plain Text          |
___
