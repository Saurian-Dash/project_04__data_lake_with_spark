# Sparkify Data Lake
#### A data lake and dimensional data model hosted on AWS

Sparkify Analytics is an application which creates a dimensional data model in an Amazon Redshift database to serve the analytical needs of Sparkify. The dimension model allows us to slice our songplay event data by many dimensions (categories) to discover new insights and trends.

With up-to-date analysis of user behaviour, we can ride trends as they happen and drive up user engagement.

The dimensional model has been designed with accessibility in mind; business users will spend less time preparing the data and more time gaining insights from it.
___

## Setup Instructions

### Create an IAM User
- Sign in to the [IAM console](https://console.aws.amazon.com/iam/) with your AWS credentials.
- From the navigation panel, select **Users**, then click the **Add user** button.
- Enter a **User name**.
- Enable **Programmatic access** (this is vital for the application to function).
- Click **Next: Permissions** and select **Attach existing policies directly**.
- Enter "iam" in the **Filter policies** field and select **IAMFullAccess** by clicking its checkbox.
- Enter "redshift" in the **Filter policies** field and select **AmazonRedshiftFullAccess** by clicking its checkbox.
- Enter "s3" in the **Filter policies** field and select **AmazonS3ReadOnlyAccess** by clicking its checkbox.
- Click **Next: Tags**, this application does not make use of tags, so skip this section by clicking **Next: Review**.
- Check that all 3 policies are present and click **Create user**.
- You should see a "Success" message and an option to **Download .csv**. Ensure you download the .csv file and store it securely, it is not possible to retrieve the IAM user credentials beyond this point.


### Update Application Settings
- Open the `dwh.cfg` file in the application's `settings` directory with a text editor.
- You will find a set of configuration options for **IAM**, **REDSHIFT** and **S3**. In the **IAM** section, enter the `AWS_KEY` and `AWS_SECRET` credentials of the IAM user created previously.
- Enter your preferred [AWS region](https://docs.aws.amazon.com/general/latest/gr/rande.html) if you want to change the `us-west-2` application default.
- Save and close the file.

#### *Note: Values in the `dwh.cfg` file must **not** be enclosed in quotes.*
___

## Operation Instructions

### Application Structure
This application consists of two concepts; `Operators` and `Manifests`:
- `Operators` are classes designed to interface with a particular API and execute operations.
- Manifests are objects that contain all of the information required to carry out an operation.

#### IAMOperator
The `IAMOperator` creates the AWS Role and attaches the policies which permit the application to interact with AWS resources. Once an AWS Role is created it can be referenced by its `ARN` (Amazon Role Name). This reference is assigned to the `dwh_role_arn` property of the IamOperator class, so that it can be accessed by other parts of the application.

#### RedshiftOperator
The `RedshiftOperator` creates a Redshift cluster with the Role `ARN` provided by the `IAMOperator`. The cluster configuration is declared in the application config file `settings/dwh.cfg`, the default setting is a dc2.large 4-node cluster.

Once the cluster is created, its endpoint will be assigned as a property of `RedshiftOperator` class and logged to the terminal. Retain this endpoint so that you may connect to the cluster via an external database client with the credentials found in the application config file `settings/dwh.cfg`.

#### PostgreSQLOperator
The PostGreSQLOperator connects to the cluster endpoint provided by the RedshiftOperator to execute SQL statements on the database. This class is responsible for all database operations in the application

### Running the Application
This application requires Python 3 to run and assumes you have your Python path configured to start Python with `python`. Please amend the suggested commands accordingly to match your setup.

Open a terminal and navigate to the application directory. Enter one of the the following commands before pressing enter to start the application:

#### Dry Run Mode
- Dry Run mode: `python app.py --dry_run`

Starting the application in `dry_run` mode will teardown the AWS infrastructure and Redshift cluster upon completion of the ETL process. This mode has been implemented for testing purposes when you do not want to leave the cluster running and incurring costs.

#### Live Mode
- Live mode: `python app.py --live`

Starting the application in `live` mode will retain the AWS infrastructure and Redshift cluster upon completion of the ETL process. Note that the cluster will begin incurring costs beyond this point, so be sure that you are ready to go live before running this command.

#### ETL Process
The application will create all of the required AWS resources to spin up a Redshift cluster. Once the cluster is available, a PostgreSQL client will be used to connect to the database and execute SQL commands to:

- Create the database schema and tables.
- Read raw data from S3 into staging tables.
- Clean and transform the staged data
- Load the clean data to the dimensional model.

#### Cluster Endpoint
The cluster endpoint will be logged in the terminal during runtime. Take note of this so that you can connect to the database with an external client using the cedentials found in the application's config file `settings/dwh.cfg`.

Once the ETL operation is complete, you can connect to the database via an external PostgreSQL client with the cluster endpoint and user credentials.

You can also retrieve the endpoint or make a connection to the database from the [Redshift Console](https://us-west-2.console.aws.amazon.com/redshiftv2/):

#### Retrieve Endpoint
- Navigate to **Clusters** and click the `sparkifydb-cluster`.
- Click **Properties** and you will find the endpoint displayed in the **Connection details** panel.

#### Connect to Database
- Navigate to **Editor**.
- Enter the database name, username and password found in the application config file.
- Click **Connect to database**.
___


## Dimensional Data Model

### Data Compression

#### ZSTD
For text, `ZSTD` encoding is used due to its high compression ratio and good query performance. `ZSTD` works especially well with columns which contain a mixture of long and short strings, the descriptive fields in our data very much fit this description.


#### AZ64
For dates, timestamps and numerics, `AZ64` compression is used due to its high compression ratio and improved query performance for these data types. Since our fact table is sorted on event date and we often query by date, we can take advantage of `AZ64`'s high compression ratio and parallel processing performance to save on storage and compute time.

### Data Distribution
The data distribution in Redshift is determined by the `DISTKEY` and is sorted by the `SORTKEY`. The `DISTKEY` determines how the records are distributed among the clusters, while the `SORTKEY` is the column the records are sorted by. The `sparkifydb` tables have been designed to optimise query performance when slicing and filtering events by time.

#### Fact Table
Since we will be querying recent data frequently to keep up to date with user behaviour, the event timestamp of the `fact_songplays` table is set as the `DISTKEY` and `SORTKEY`. This is so that time filtered queries can [skip entire blocks of the data](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html) that fall outside of the time range, drastically reducing the number of records the query runner needs to scan.

#### Dimension Tables
To add detail to the events data, or to slice and aggregate by, we must frequently join the fact table to the dimension tables. In this situation, setting the join column as the `DISTKEY` and `SORTKEY` improves query performance as the query runner does not need to [pre-sort](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html) the data before joining.
___


## Database Schema

![sparkifydb_schema](https://pasteboard.co/IQpWbdc.png)

#### Data Warehouse Vaults
The database contains two schemas; `raw_vault` and `public_vault`. The `raw_vault` contains the staging tables for data loaded from S3 and the `public_vault` contains the dimensional model. Be sure to query the dimensional model either by setting your client's `search_path` to `public_vault` or prefixing the table references in your queries with `public_vault`.

#### Table Name: `dim_artists`
- Dist key: `artist_id`
- Sort key: `artist_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `artist_id (PK)`    | VARCHAR             | Plain Text          |
| `name`              | VARCHAR             | Plain Text          |
| `location`          | VARCHAR             | Plain Text          |
| `latitude`          | NUMERIC             | Decimal Number      |
| `longitude`         | NUMERIC             | Decimal Number      |

#### Table Name: `dim_songs`
- Dist key: `song_id`
- Sort key: `song_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `song_id (PK)`      | VARCHAR             | Plain Text          |
| `artist_id`         | VARCHAR             | Plain Text          |
| `title`             | VARCHAR             | Plain Text          |
| `year`              | SMALLINT            | Whole Number        |
| `duration`          | NUMERIC             | Decimal Number      |

#### Table Name: `dim_time`
- Dist key: `time_id`
- Sort key: `time_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `time_id (PK)`      | BIGINT              | Whole Number        |
| `start_time`        | TIMESTAMP           | Date and Time       |
| `hour`              | SMALLINT            | Whole Number        |
| `day`               | SMALLINT            | Whole Number        |
| `week`              | SMALLINT            | Whole Number        |
| `month`             | SMALLINT            | Whole Number        |
| `year`              | SMALLINT            | Whole Number        |
| `weekday`           | SMALLINT            | Whole Number        |
___

#### Table Name: `dim_users`
- Dist key: `user_id`
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
- Dist key: `time_id`
- Sort key: `time_id`

| Dimension           | Data Type           | Output              |
|---------------------|---------------------|---------------------|
| `songplay_id (PK)`  | BIGINT              | Whole Number        |
| `time_id`           | BIGINT              | Whole Number        |
| `start_time`        | TIMESTAMP           | Date and Time       |
| `user_id`           | BIGINT              | Whole Number        |
| `level`             | VARCHAR             | Plain Text          |
| `song_id`           | VARCHAR             | Plain Text          |
| `artist_id`         | VARCHAR             | Plain Text          |
| `session_id`        | BIGINT              | Whole Number        |
| `location`          | VARCHAR             | Plain Text          |
| `user_agent`        | VARCHAR             | Plain Text          |
___

## Sample Queries

Execute `SET search_path TO public_vault;` to select the `public_vault` in your PostgreSQL client;

#### Top 10 Songs Over a Year Using the Time Dimension
```
SELECT
	s.title,
    a.name,
    Count(*) AS songplays
FROM public_vault.fact_songplays f
	JOIN public_vault.dim_artists a
    	ON a.artist_id = f.artist_id
    JOIN public_vault.dim_songs s
    	ON s.song_id = f.song_id
    JOIN public_vault.dim_time t
    	ON t.time_id = f.time_id
WHERE t.year = 2018
GROUP BY 1, 2
ORDER BY songplays DESC
LIMIT 10;
```

#### Top 10 Songs Over a Week Using the Time Dimension
```
SELECT
	s.title,
    a.name,
    Count(*) AS songplays
FROM public_vault.fact_songplays f
	JOIN public_vault.dim_artists a
    	ON a.artist_id = f.artist_id
    JOIN public_vault.dim_songs s
    	ON s.song_id = f.song_id
    JOIN public_vault.dim_time t
    	ON t.time_id = f.time_id
WHERE t.year = 2018
	AND t.week = 46
GROUP BY 1, 2
ORDER BY songplays DESC
LIMIT 10;
```

#### Total Songplays by Day
```
SELECT
	f.start_time :: DATE,
    Count(*) AS songplays
FROM public_vault.fact_songplays f
	JOIN public_vault.dim_time t
    	ON t.time_id = f.time_id
WHERE t.year = 2018
	AND t.month = 11
GROUP BY 1
ORDER BY 1;
```


#### Total Users and Demographic Split by Location
```
SELECT 
	f.location,
	100 * SUM(CASE WHEN u.gender = 'M' THEN 1 ELSE 0 END) / COUNT(*) AS male_pc,
    100 * SUM(CASE WHEN u.gender = 'F' THEN 1 ELSE 0 END) / COUNT(*) AS female_pc,
    COUNT(*) AS total_users
from public_vault.fact_songplays f
	JOIN public_vault.dim_users u
    	ON u.user_id = f.user_id
GROUP BY 1
ORDER BY 4 DESC;
```
