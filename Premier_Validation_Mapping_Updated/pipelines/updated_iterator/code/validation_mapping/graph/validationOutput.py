from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *

def validationOutput(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .option("overwriteSchema", True)\
        .mode("append")\
        .saveAsTable(f"`{Config.validationOutputCatalog}`.`{Config.validationOutputSchema}`.`{Config.validationOutputTable}`")
