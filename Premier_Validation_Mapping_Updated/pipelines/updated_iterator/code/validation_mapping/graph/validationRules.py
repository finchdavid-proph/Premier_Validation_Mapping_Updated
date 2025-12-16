from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *

def validationRules(spark: SparkSession) -> DataFrame:
    return spark.read.table(
        f"`{Config.validationRulesCatalog}`.`{Config.validationRulesSchema}`.`{Config.validationRulesTable}`"
    )
