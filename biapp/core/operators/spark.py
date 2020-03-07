import os

from pyspark.sql import functions as f
from pyspark.sql import SparkSession

import biapp.core.logger.log as log

logger = log.setup_custom_logger(__name__)


class SparkOperator:

    def __init__(self):

        self.session = self.create_spark_session()

    def clean_dataframe(self, df, *args, **kwargs):
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

    def create_spark_session(self, *args, **kwargs):
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
            )
            .config(
                'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version',
                '2',
            )
            .config(
                'spark.hadoop.fs.s3a.impl',
                'org.apache.hadoop.fs.s3a.S3AFileSystem'
            )
            .config('spark.hadoop.fs.s3a.multiobjectdelete.enable', 'false')
            .config('spark.hadoop.fs.s3a.fast.upload', 'true')
            .config('spark.sql.parquet.filterPushdown', 'true')
            .config('spark.sql.parquet.mergeSchema', 'false')
            .config('spark.speculation', 'false')
            .getOrCreate())

        session.conf.set(
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation',
            'true',
        )

        return session

    def execute_sql(self, df, query, *args, **kwargs):
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
        df.createOrReplaceTempView('stage')

        logger.info(f"Executing SQL: {query['name']} \n{query['sql']}")
        df = self.session.sql(query['sql'])

        return df

    def stage_json_data(self,
                        input_data,
                        schema,
                        query,
                        table_name,
                        *args,
                        **kwargs):
        """
        Reads input data files from the specified path with the specified file
        extension into a Spark DataFrame. The specified SQL query is excuted
        to clean the DataFrame and the result is returned as a new DataFrame.

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
                            *args,
                            **kwargs):
        """
        Writes a DataFrame to the specified path as parquet files, partitioned
        by the specified columns. If no partition is provided, the Dataframe
        will be coalesced to a single file.

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
            (df.repartition(*partition)
               .write
               .mode(mode)
               .partitionBy(*partition)
               .parquet(os.path.join(output_path, table_name)))
        else:
            (df.coalesce(1)
               .write
               .mode(mode)
               .parquet(os.path.join(output_path, table_name)))

        logger.info(
            f'Table: {table_name}'
            f' | partioned by: {partition}'
            f' | written to: {output_path}/{table_name}'
        )
