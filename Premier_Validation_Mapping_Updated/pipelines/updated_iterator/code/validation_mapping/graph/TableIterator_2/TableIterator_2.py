from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from validation_mapping.functions import *
from . import *
from .config import *


class TableIterator_2(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_sourceTable_1 = sourceTable_1(spark)
        df_select_configured_columns_1 = select_configured_columns_1(spark, df_sourceTable_1)
        df_expandValidationRules_1 = expandValidationRules_1(spark, df_select_configured_columns_1)
        df_generateFinalSchema_1 = generateFinalSchema_1(spark, df_expandValidationRules_1)
        subgraph_config.update(Config)

        return list((df_generateFinalSchema_1, ))

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> DataFrame:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("source_catalog", "source_catalog"),  ("source_schema", "source_schema"),  ("source_table", "source_table"),              ("table_key", "table_key"),  ("validation_run_id", "validation_run_id"),              ("validation_timestamp", "validation_timestamp"),  ("source_columns", "source_columns"),              ("validations", "validations")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
