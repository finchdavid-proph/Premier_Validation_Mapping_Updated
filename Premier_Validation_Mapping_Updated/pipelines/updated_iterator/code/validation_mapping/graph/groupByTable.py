from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *

def groupByTable(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("source_catalog"), col("source_schema"), col("source_table"), col("table_key"))

    return df1.agg(
        max(col("validation_run_id")).alias("validation_run_id"), 
        max(col("validation_timestamp")).alias("validation_timestamp"), 
        collect_list(col("source_columns")).alias("source_columns"), 
        collect_list(struct(col("source_columns").alias("cols"), col("logic"))).alias("validations")
    )
