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
    """
    TODO: Docstring
    """
    def __init__(self):

        self.session = self.create_spark_session()

    def clean_dataframe(self, df, **kwargs):
        """
        TODO: Docstring
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
        TODO: Docstring
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
        TODO: Docstring
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
        TODO: Docstring
        """
        df = self.session.read.json(input_data, schema)
        df = self.clean_dataframe(df)
        df = self.execute_sql(df=df, query=query)

        df.createOrReplaceTempView(table_name)

        return df

    def write_parquet_file(self,
                           df,
                           output_path,
                           table_name,
                           partition,
                           mode='overwrite',
                           **kwargs):
        """
        TODO: Docstring
        """
        try:
            (df.write
               .partitionBy(partition)
               .format('parquet')
               .mode(mode)
               .saveAsTable(table_name))
        except Exception as e:
            raise e

        logger.info(
            f'Files partioned by: {partition} | written to: {output_path}'
        )
