from prophecy.config import ConfigBase


class Validations(ConfigBase):
    def __init__(self, prophecy_spark=None, col: str=None, logic: str=None, prophecy_project_config=None, **kwargs):
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config, kwargs)
        self.col = col
        self.logic = logic
        pass


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            source_catalog: str="",
            source_schema: str="",
            source_table: str="",
            table_key: str="",
            validation_run_id: str="",
            validation_timestamp: str="01-01-1970T00:00:00",
            validations: list=None,
            prophecy_project_config=None,
            **kwargs
    ):
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config, kwargs)
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.source_table = source_table
        self.table_key = table_key
        self.validation_run_id = validation_run_id
        self.validation_timestamp = self.get_timestamp_value(validation_timestamp)
        self.validations = self.get_config_object(prophecy_spark, [], validations, Validations)
        pass

    def update(self, updated_config):
        self.source_catalog = updated_config.source_catalog
        self.source_schema = updated_config.source_schema
        self.source_table = updated_config.source_table
        self.table_key = updated_config.table_key
        self.validation_run_id = updated_config.validation_run_id
        self.validation_timestamp = updated_config.validation_timestamp
        self.validations = updated_config.validations
        self.update_project_config_fields(updated_config)
        pass

Config = SubgraphConfig()
