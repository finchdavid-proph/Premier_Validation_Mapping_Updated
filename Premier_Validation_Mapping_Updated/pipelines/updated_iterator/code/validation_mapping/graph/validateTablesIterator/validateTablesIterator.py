from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from validation_mapping.functions import *
from . import *
from .config import *


class validateTablesIterator(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_sourceTable = sourceTable(spark)
        df_expandValidationRules = expandValidationRules(spark, df_sourceTable)
        df_generateFinalSchema = generateFinalSchema(spark, df_expandValidationRules)
        subgraph_config.update(Config)

        return list((df_generateFinalSchema, ))

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> DataFrame:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("source_catalog", "source_catalog"),  ("source_schema", "source_schema"),  ("source_table", "source_table"),              ("table_key", "table_key"),  ("validation_run_id", "validation_run_id"),              ("validation_timestamp", "validation_timestamp"),  ("validations", "validations")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
