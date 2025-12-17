from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *

def extract_required_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("source_catalog"), 
        col("source_schema"), 
        col("source_table"), 
        col("table_key"), 
        col("validation_run_id"), 
        col("validation_timestamp"), 
        array_distinct(flatten(transform(col("validations"), lambda v: v.getField("cols")))).alias("source_columns"), 
        col("validations")
    )
