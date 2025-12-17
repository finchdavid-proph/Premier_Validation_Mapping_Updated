from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *

def addMetadata(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("source_catalog"), 
        col("source_schema"), 
        col("source_table"), 
        col("source_columns"), 
        col("logic"), 
        concat(col("source_catalog"), lit("."), col("source_schema"), lit("."), col("source_table")).alias("table_key"), 
        concat(lit("validation_run_"), unix_timestamp(current_timestamp()).cast(StringType()))\
          .alias("validation_run_id"), 
        current_timestamp().alias("validation_timestamp")
    )
