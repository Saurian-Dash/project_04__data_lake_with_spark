import os

from pyspark.sql import functions as f
from pyspark.sql import SparkSession

import core.logger.log as log

logger = log.setup_custom_logger(__name__)


class SparkOperator:

    def __init__(self):

        self.session = self.create_spark_session()

    def clean_dataframe(self, df, *args, **kwargs):
        """
        Removes leading and trailing whitespace from DataFrame values and
        replaces empty strings with None. The resulting clean DataFrame is
        returned as a new DataFrame.

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
        Creates a PySpark SparkSession configured to run on AWS EMR.

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
                'spark.sql.legacy'
                '.allowCreatingManagedTableUsingNonemptyLocation',
                'true',
            )
            .getOrCreate())

        return session

    def execute_sql(self, df, query, *args, **kwargs):
        """
        Creates a temporary view of the input DataFrame and executes an SQL
        query on it, returning the result as a new DataFrame.

        Args:
            df (pyspark.DataFrame):
                The PySpark DataFrame to query with SQL.

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
        Reads input data files with the specified path and extension into a
        PySpark DataFrame. The specified SQL query is excuted to clean the
        data and the result is returned as a new DataFrame.

        Args:
            input_data (str) | (list):
                Input filepath or directory path containg the input data.

            schema (pyspark.StuctType):
                Spark schema describing the structure of the input data.

            query (str):
                Spark SQL query to execute to clean the input data.

            table_name (str):
                The name of the resulting clean table.

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
        by the specified columns. If no partition is provided, the output will
        be coalesced to a single partition.

        Args:
            df (pyspark.Dataframe):
                The Spark DataFrame to write to parquet files.

            output_path (str):
                The filepath for output parquet files.

            table_name (str):
                The name of the DataFrame to write to parquet files

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
