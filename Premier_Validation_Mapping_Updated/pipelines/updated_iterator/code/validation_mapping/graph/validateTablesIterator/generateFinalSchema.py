from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_mapping.functions import *

def generateFinalSchema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(Config.validation_run_id).alias("validation_run_id"), 
        lit(Config.validation_timestamp).alias("validation_timestamp"), 
        lit(Config.source_catalog).alias("source_catalog"), 
        lit(Config.source_schema).alias("source_schema"), 
        lit(Config.source_table).alias("source_table"), 
        col("source_column"), 
        col("validation_result"), 
        col("validation_logic"), 
        col("row_count"), 
        col("validation_duration_ms")
    )
