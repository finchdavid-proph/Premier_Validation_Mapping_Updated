from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_mapping.functions import *

def expandValidationRules(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    from pyspark.sql.functions import expr
    import time
    source_df = in0
    results = []
    row_count = source_df.count()

    for validation in Config.validations:
        source_column = validation.col
        logic = validation.logic
        validation_start_time = time.time()

        try:
            result_df = source_df.select(expr(logic).alias('validation_result'))
            result_value = result_df.collect()[0]['validation_result']
            result_str = str(result_value) if result_value is not None else "NULL"
        except Exception as e:
            result_str = f'ERROR: {str(e)}'
        finally:
            validation_end_time = time.time()
            validation_duration_ms = int((validation_end_time - validation_start_time) * 1000)

        results.append(
            {
              'source_column': source_column,
              'validation_result': result_str,
              'validation_logic': logic,
              'row_count': row_count,
              'validation_duration_ms': validation_duration_ms
            }
        )

    # Minimal schema - just dynamic columns
    simple_schema = StructType([
            StructField('source_column', StringType()),
            StructField('validation_result', StringType()),
            StructField('validation_logic', StringType()),
            StructField('row_count', LongType()),
            StructField('validation_duration_ms', LongType())

    ])
    out0 = spark.createDataFrame(results, schema = simple_schema)

    return out0
