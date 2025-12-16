from setuptools import setup, find_packages
setup(
    name = 'validation_mapping',
    version = '1.0',
    packages = find_packages(include = ('validation_mapping*', )) + ['prophecy_config_instances.validation_mapping'],
    package_dir = {'prophecy_config_instances.validation_mapping' : 'configs/resources/validation_mapping'},
    package_data = {'prophecy_config_instances.validation_mapping' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.7'],
    entry_points = {
'console_scripts' : [
'main = validation_mapping.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
