from validation_mapping.graph.validateTableIterator.config.Config import SubgraphConfig as validateTableIterator_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, validateTableIterator: dict=None, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(validateTableIterator, prophecy_project_config, **kwargs)

    def update(self, validateTableIterator: dict={}, prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        self.validateTableIterator = self.get_config_object(
            prophecy_spark, 
            validateTableIterator_Config(
              prophecy_spark = prophecy_spark, 
              prophecy_project_config = prophecy_project_config
            ), 
            validateTableIterator, 
            validateTableIterator_Config, 
            prophecy_project_config
        )
        pass
