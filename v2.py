
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.profile import BasicSuiteBuilderProfiler
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

#First load your table data
datasource0 = glueContext.create_dynamic_frame.from_catalog(
           database = "dataquality",
           table_name = "wholesale",
           transformation_ctx = "datasource0")

#Convert to spark DataFrame 
df1 = datasource0.toDF()
print(df1)

#Initiate great_expectations context
data_context_config = DataContextConfig(
    config_version=2,
    plugins_directory=None,
    config_variables_file_path=None,
    datasources={
        "my_spark_datasource": {
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "SparkDFDatasource",
            "module_name": "great_expectations.datasource",
            "batch_kwargs_generators": {
                "example": {
                    "class_name": "S3GlobReaderBatchKwargsGenerator",
                    "bucket": "dataquality-dc", #Bucket with the data to profile
                    "assets": {
                        "example_asset": {
                            "prefix": "wholesale/", #Prefix of the data to profile. Trailing slash is important
                            "regex_filter": ".*csv", #Filter by type csv, parquet....
                        }
                    },
                },
            },
        }
    },
    stores={
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "dataquality-dc", #Bucket storing great_expectations suites results
                "prefix": "great_expectations/expectations/", #Trailing slash is important
            },
        },
        "validations_S3_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "dataquality-dc",
                "prefix": "great_expectations/validations/"
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_S3_store",
    validations_store_name="validations_S3_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    store_backend_defaults=S3StoreBackendDefaults(default_bucket_name="dataquality-dc"),
)
print("GE DATA CONTEXT CONFIG", data_context_config)

context = BaseDataContext(project_config=data_context_config)

print("GE BASE DATA CONTEXT", context)
print("DATASOURCES", context.list_datasources())

batch_kwargs = {'dataset': df1, 'datasource': 'my_spark_datasource'}

expectation_suite_name = "profiling.demo"

suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

