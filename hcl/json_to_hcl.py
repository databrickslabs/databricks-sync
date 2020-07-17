import json
from ctypes import *
from typing import Text, Any, Dict

lib = cdll.LoadLibrary("./json2hcl.so")


# Python representation of the C struct for CreateResourceHCLResponse
class CreateResourceHCLResponse(Structure):
    _fields_ = [("hcl", c_char_p), ("error", c_char_p)]


lib.CreateResourceHCL.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p]
lib.CreateResourceHCL.restype = CreateResourceHCLResponse


def create_hcl_from_json(object_type: Text, object_name: Text, object_identifier: Text,
                         object_data_dictionary: Dict[Text, Any]) -> Text:
    c_object_type = c_char_p(object_type.encode("UTF-8"))
    c_object_name = c_char_p(object_name.encode("UTF-8"))
    c_object_identifier = c_char_p(object_identifier.encode("UTF-8"))
    # Load object as dictionary
    json_str = json.dumps(object_data_dictionary)
    c_json_str = c_char_p(json_str.encode("UTF-8"))
    output = lib.CreateResourceHCL(c_object_type, c_object_name, c_object_identifier, c_json_str)
    if len(output.error) > 0:
        raise ValueError(output.error.decode("UTF-8"))
    else:
        return output.hcl.decode("UTF-8")


object = "resource"
type = "cluster"
name = "my_cluster_cluster-1243424e"
# resource_data = {
#     "@block:block_test": {
#         "@block:super_nested": {
#             "nested_sub_sub": 123
#         },
#         "hello_world": 123,
#         "mappy": {
#             "nested_sub_sub_sub": 123,
#             "test1234": 123
#         }
#     },
#     "test": 12345,
#     "test2": "string ${upper.lib}"
# }
resource_data = {
    "name": "Nightly model training",
    "new_cluster": {
        "spark_version": "5.3.x-scala2.11",
        "node_type_id": "r3.xlarge",
        "aws_attributes": {
            "availability": "ON_DEMAND"
        },
        "num_workers": 10
    },
    "libraries": [
        {
            "jar": "dbfs:/my-jar.jar"
        },
        {
            "maven": {
                "coordinates": "org.jsoup:jsoup:1.7.2"
            }
        }
    ],
    "timeout_seconds": 3600,
    "max_retries": 1,
    "schedule": {
        "quartz_cron_expression": "0 15 22 ? * *",
        "timezone_id": "America/Los_Angeles"
    },
    "spark_jar_task": {
        "main_class_name": "com.databricks.ComputeModels"
    }
}
# jsonStr = '{"@block:block_test":{"@block:super_nested":{"nested_sub_sub":123},"hello_world":123,"@block:mappy":{"nested_sub_sub_sub":123,"test1234":123}},"test":12345,"test2":"string ${upper.lib}"}'

output = create_hcl_from_json(object, type, name, resource_data)

print(output)
