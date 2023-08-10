import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import yaml
import boto3
import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    S3StoreBackendDefaults,
)
from great_expectations.util import get_context

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client("s3")
response = s3_client.get_object(
    Bucket="dataquality-dc", Key="great_expectations/great_expectations.yaml"
)
config_file = yaml.safe_load(response["Body"])

config = DataContextConfig(
    config_version=config_file["config_version"],
    datasources=config_file["datasources"],
    expectations_store_name=config_file["expectations_store_name"],
    validations_store_name=config_file["validations_store_name"],
    evaluation_parameter_store_name=config_file["evaluation_parameter_store_name"],
    plugins_directory="/great_expectations/plugins",
    stores=config_file["stores"],
    data_docs_sites=config_file["data_docs_sites"],
    config_variables_file_path=config_file["config_variables_file_path"],
    anonymous_usage_statistics=config_file["anonymous_usage_statistics"],
    checkpoint_store_name=config_file["checkpoint_store_name"],
    store_backend_defaults=S3StoreBackendDefaults(
        default_bucket_name=config_file["data_docs_sites"]["s3_site"]["store_backend"]["bucket"]
    ),
)
context_gx = get_context(project_config=config)

expectation_suite_name = str(datetime.datetime.now())
suite = context_gx.add_expectation_suite(expectation_suite_name)
batch_request = RuntimeBatchRequest(
    datasource_name="spark_s3",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="sample",
    batch_identifiers={"default_identifier_name": "default_identifier"},
    runtime_parameters={"path": "s3a://dataquality-dc/wholesale/sales.csv"},
)
validator = context_gx.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
print('------------------------------------------------------------')
print(validator.head())
print('------------------------------------------------------------')

validator.expect_column_values_to_not_be_null(
    column="Region"
)
validator.save_expectation_suite(discard_failed_expectations=False)
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_{expectation_suite_name}", context_gx, **checkpoint_config
)
results = checkpoint.run(result_format="SUMMARY", run_name="test")
validation_result_identifier = results.list_validation_result_identifiers()[0]
