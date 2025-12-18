from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_mapping.config.ConfigStore import *
from validation_mapping.functions import *
from prophecy.utils import *
from validation_mapping.graph import *

def pipeline(spark: SparkSession) -> None:
    df_validationRules = validationRules(spark)
    df_addMetadata = addMetadata(spark, df_validationRules)
    df_groupByTable = groupByTable(spark, df_addMetadata)
    df_extractRequiredColumns = extractRequiredColumns(spark, df_groupByTable)
    df_validateTableIterator = validateTableIterator(Config.validateTableIterator)\
                                   .apply(spark, df_extractRequiredColumns)
    validationOutput(spark, df_validateTableIterator)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_mapping").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/updated_iterator")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/updated_iterator", config = Config)(pipeline)

if __name__ == "__main__":
    main()
