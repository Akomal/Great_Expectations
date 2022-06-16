from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier

from pyspark.sql import Row, SparkSession
import tkinter as tk
from tkinter import filedialog

context = ge.data_context.DataContext()


#add datasource
base_directory = input("Enter base directory where data is stored locally like /home/.. ")
datasource_config = {
    "name": "datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": base_directory,
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\\.csv",
            },
        },
    },
}
context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)
#input data file
from pyspark.sql import Row, SparkSession
import tkinter as tk
from tkinter import filedialog


root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()
spark = SparkSession.builder \
    .appName("GE-spark") \
    .getOrCreate()
df = spark.read.option("header", True) \
        .csv(file_path)

#build runtime data request
batch_request = RuntimeBatchRequest(
    datasource_name="datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="claims",  # this can be anything that identifies this data_asset for you
    runtime_parameters= {"batch_data": df},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)
##create an expectation suite
expectaion_suite=context.create_expectation_suite(
    expectation_suite_name="expectation_suite", overwrite_existing=True
)
# Create an Expectation
expectation_configuration = ExpectationConfiguration(
   # Name of expectation type being added
   expectation_type="expect_column_values_to_not_be_null",
   kwargs={
      "column": "Id"
   },
   meta={
      "notes": {
         "format": "markdown",
         "content": "Null value check`"
      }
   }
)
expectaion_suite.add_expectation(expectation_configuration=expectation_configuration)
expectation_configuration = ExpectationConfiguration(
   expectation_type="expect_column_values_to_be_in_set",
   kwargs={
      "column": "STATUSP",
      "value_set": ["CLOSED", "OPEN"]
   },
   # Note optional comments omitted
)
expectaion_suite.add_expectation(expectation_configuration=expectation_configuration)
# Add the Expectation to the suite
context.save_expectation_suite(expectaion_suite, "claims_suite")
#initialize validator
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="claims_suite"
)
result = context.run_validation_operator("action_list_operator", assets_to_validate=[validator])

validation_re_identifier = result.list_validation_result_identifiers()[0]
#build data docs
context.build_data_docs()
context.open_data_docs()# give this command in terminal to view data docs great_expectations docs build

if not result.success:
        print("issues")