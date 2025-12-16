from validation_mapping.graph.validateTablesIterator.config.Config import (
    SubgraphConfig as validateTablesIterator_Config
)
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, validateTablesIterator: dict=None, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(validateTablesIterator, prophecy_project_config, **kwargs)

    def update(self, validateTablesIterator: dict={}, prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        self.validateTablesIterator = self.get_config_object(
            prophecy_spark, 
            validateTablesIterator_Config(
              prophecy_spark = prophecy_spark, 
              prophecy_project_config = prophecy_project_config
            ), 
            validateTablesIterator, 
            validateTablesIterator_Config, 
            prophecy_project_config
        )
        pass
