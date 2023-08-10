from ruamel import yaml
import yaml as yml
from pathlib import Path
import datetime
import great_expectations as gx
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    S3StoreBackendDefaults,
)
print('-----------------------------------------')
print('hey')
print('-----------------------------------------')
datasource_config = {
    "name": "my_s3_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetS3DataConnector",
            "bucket": "dataquality-dc",
            "prefix": "wholesale/",
            "default_regex": {
                "pattern": "(.*)\\.csv",
                "group_names": ["data_asset_name"],
            },
        },
    },
}
context_gx = gx.get_context()
print('-----------------------------------------')
print('contex')
print('-----------------------------------------')
context_gx.test_yaml_config(yaml.dump(datasource_config))
context_gx.add_datasource(**datasource_config)
print('-----------------------------------------')
print('tested')
print('-----------------------------------------')
expectation_suite_name = str(datetime.datetime.now())
suite = context_gx.add_expectation_suite(expectation_suite_name)
print('-----------------------------------------')
print('suite')
print('-----------------------------------------')
batch_request = RuntimeBatchRequest(
    datasource_name="my_s3_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="sales",
    batch_identifiers={"default_identifier_name": "default_identifier"},
    runtime_parameters={"path": "s3a://dataquality-dc/wholesale/sales.csv"},
)
print('-----------------------------------------')
print('request')
print('-----------------------------------------')
validator = context_gx.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
print('-----------------------------------------')
print('validator')
print('-----------------------------------------')
print(validator.head())
