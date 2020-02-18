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
            df (pyspark.DataFrame): The Spark DataFrame to clean.

        Returns:
            pyspark.DataFrame
        """
        # trim whitespace from all values
        for colname in df.columns:
            df = df.withColumn(colname, f.trim(f.col(colname)))

        # replace empty values with None
        for colname in df.columns:
            df = df.withColumn(
                colname,
                f.when(f.col(colname) == r'^\s*$', None)
                .otherwise(f.col(colname))
            )

        return df

    def create_spark_session(self, **kwargs):
        """
        TODO: Parametrize SparkSession config

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
                'org.apache.hadoop:hadoop-aws:2.7.5'
            ).getOrCreate())

        session.conf.set(
            'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation',
            'true'
        )

        return session

    def execute_sql(self, df, query, **kwargs):
        """
        Creates a temporary view of the input DataFrame and executes a SQL
        query on it. The query results are returned as a new DataFrame.

        Args:
            df (pyspark.DataFrame): The DataFrame to execute a SQL query on.
            query (str): The SQL query to execute on the DataFrame.

        Returns:
            pyspark.DataFrame
        """
        df.createOrReplaceTempView('stage')
        df = self.session.sql(query)

        return df

    def stage_json_data(self,
                        input_data,
                        schema,
                        query,
                        table_name,
                        **kwargs):
        """
        TODO: Implement an os.walk to generate a list of filepaths for this
        function to loop over.

        Args:
            input_data (str):
            schema (pyspark.StuctType):
            query (str):
            table_name (str):

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
                            time_partition=None,
                            mode='overwrite',
                            **kwargs):
        """
        TODO: Docstring

        Args:
            df (pyspark.Dataframe):
            output_path (str):
            table_name (str):
            partition: (tuple):
            time_partition (str):
            mode (str):
        """
        if time_partition:
            partition = ('year', 'month', 'day')
            (df.withColumn('year', f.year(f.col(time_partition)))
               .withColumn('month', f.month(f.col(time_partition)))
               .withColumn('day', f.dayofmonth(f.col(time_partition)))
               .write
               .format('parquet')
               .partitionBy(partition)
               .option('path', os.path.join(output_path, table_name))
               .saveAsTable(table_name))
        else:
            (df.write
               .format('parquet')
               .partitionBy(partition)
               .option('path', os.path.join(output_path, table_name))
               .saveAsTable(table_name))

        logger.info(
            f'Files partioned by: {partition} | written to: {output_path}'
        )
