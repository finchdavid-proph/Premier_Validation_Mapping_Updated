from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_mapping.functions import *

def select_configured_columns_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    source_columns = Config.source_columns if hasattr(Config, 'source_columns') else None

    if not source_columns or len(source_columns) == 0:
        return in0

    actual_columns = set(in0.columns)
    columns_to_select = [c for c in source_columns if c in actual_columns]

    if not columns_to_select:
        return in0

    out0 = in0.select(*[col(c) for c in columns_to_select])

    return out0
