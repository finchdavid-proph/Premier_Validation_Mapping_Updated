from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_mapping.functions import *

def sourceTable(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.source_catalog}`.`{Config.source_schema}`.`{Config.source_table}`")
