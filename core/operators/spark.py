import configparser
import os

from pyspark.sql import functions as f
from pyspark.sql import SparkSession

import core.logger.log as log

logger = log.setup_custom_logger(__name__)

config = configparser.ConfigParser()
config.read('settings/envs.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get(
    'AWS', 'AWS_ACCESS_KEY_ID'
)
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'AWS', 'AWS_SECRET_ACCESS_KEY'
)


class SparkOperator:

    def __init__(self):

        self.session = self.create_spark_session()

    def clean_dataframe(self, df, **kwargs):
        """
        Data cleaning function which removes leading and trailing whitespace
        from DataFrame values, replacing empty strings with Python None. The
        resulting clean DataFrame is returned as a new DataFrame.

        Args:
            df (pyspark.DataFrame):
                The Spark DataFrame to clean.

        Returns:
            pyspark.DataFrame
        """
        for colname in df.columns:
            df = df.withColumn(colname, f.trim(f.col(colname)))

        for colname in df.columns:
            df = df.withColumn(
                colname,
                f.when(f.col(colname) == r'^\s*$', None)
                .otherwise(f.col(colname))
            )

        return df

    def create_spark_session(self, **kwargs):
        """
        Creates a SparkSession and attaches it to self. The SparkSession
        enables the application to create and manipulate Spark DataFrames,
        keeping track of any tables or temporary views created within the
        session.

        Returns:
            pyspark.SparkSession
        """
        session = (
            SparkSession
            .builder
            .config(
                'spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:2.7.5',
            ).getOrCreate())

        session.conf.set(
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation',
            'true',
        )

        session._jsc.hadoopConfiguration().set(
            'mapreduce.fileoutputcommitter.algorithm.version',
            '2',
        )

        return session

    def execute_sql(self, df, query, **kwargs):
        """
        Creates a temporary view of the input DataFrame and executes a SQL
        query on it. The query results are returned as a new DataFrame.

        Args:
            df (pyspark.DataFrame):
                The Spark DataFrame to query.

            query (str):
                The SQL query to execute on the DataFrame.

        Returns:
            pyspark.DataFrame
        """
        logger.info(f'Creating staging table: {df}')
        df.createOrReplaceTempView('stage')

        logger.info(f'Executing SQL: \n{query}')
        df = self.session.sql(query)

        return df

    def stage_json_data(self,
                        input_data,
                        schema,
                        query,
                        table_name,
                        **kwargs):
        """
        Reads input data files from the specified path with the speficied file
        extension into a Spark DataFrame. The specified Spark SQL query is
        excuted to clean the DataFrame and the result is saved to the current
        Spark session and returned as a new DataFrame.

        Args:
            input_data (str) | (list):
                Input filepath or directory path containg the input data, you
                may pass in the path to a single file or a path to a directory
                containing many files.

            schema (pyspark.StuctType):
                Spark schema describing the structure of the input data.

            query (str):
                Spark SQL query to execute to clean the input data.

            table_name (str):
                The name of the table to stage in a temporary view.

        Returns:
            pyspark.DataFrame
        """
        df = self.session.read.json(input_data, schema)
        df = self.clean_dataframe(df)
        df = self.execute_sql(df=df, query=query)

        df.createOrReplaceTempView(table_name)

        return df

    def write_parquet_files(self,
                            df,
                            output_path,
                            table_name,
                            partition=None,
                            mode='overwrite',
                            **kwargs):
        """
        Writes a DataFrame to the specified path as parquet files, partitioned
        by the specified columns. The resulting table is saved to the current
        Spark session so that it can be referenced later if needed.

        Args:
            df (pyspark.Dataframe):
                The Spark DataFrame to write to parquet.

            output_path (str):
                The filepath for written parquet files.

            table_name (str):
                The name of the DataFrame to save.

            partition (tuple):
                DataFrame columns to partition the parquet files.

            mode (str):
                The write mode of the DataFrame writer.
        """
        if partition:
            (df.write
               .partitionBy(partition)
               .option('path', os.path.join(output_path, table_name))
               .saveAsTable(table_name, format='parquet', mode=mode))
        else:
            (df.write
               .option('path', os.path.join(output_path, table_name))
               .saveAsTable(table_name, format='parquet', mode=mode))

        logger.info(
            f'Files partioned by: {partition} | written to: {output_path}'
        )
