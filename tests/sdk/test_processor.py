import json

from pygrok import Grok

from databricks_terraformer.sdk.processor import BasicAnnotationProcessor, ResourceVariableBasicAnnotationProcessor, \
    MappedGrokVariableBasicAnnotationProcessor
from tests.sdk import *


class TestBasicAnnotationProcessor:
    def test_process(self, hcl_convert_data_with_no_processors):
        test_prefix = "@test:"
        data = {
            "path1": "world",
            "path2": {
                "nest2": "data"
            },
            "path3": [
                {
                    "nest": "data"
                },
                {
                    "nest": "data2"
                },
            ],
            "path4": [
                {
                    "super_nest": [
                        {"sub_nest": "hello1"},
                        {"sub_nest": "hello2"},
                    ],
                },
                {
                    "super_nest": [
                        {"sub_nest": "hello3"},
                        {"sub_nest": "hello4"},
                    ]
                },
            ]
        }
        expected_json = """
                {
                    "path2": {
                        "@test:nest2": "data"
                    },
                    "path3": [
                        {
                            "@test:nest": "data"
                        },
                        {
                            "@test:nest": "data2"
                        }
                    ],
                    "path4": [
                        {
                            "@test:super_nest": [
                                {
                                    "@test:sub_nest": "hello1"
                                },
                                {
                                    "@test:sub_nest": "hello2"
                                }
                            ]
                        },
                        {
                            "@test:super_nest": [
                                {
                                    "@test:sub_nest": "hello3"
                                },
                                {
                                    "@test:sub_nest": "hello4"
                                }
                            ]
                        }
                    ],
                    "@test:path1": "world"
                }
                """
        hcl_convert_data_with_no_processors.modify_json(data)
        bap = BasicAnnotationProcessor(test_prefix, ["path1",
                                                     "path2.nest2",
                                                     "path3.[*].nest",
                                                     "path4.[*].super_nest",
                                                     "path4.[*].super_nest.[*].sub_nest"
                                                     ])
        bap.process(hcl_convert_data_with_no_processors)

        actual = hcl_convert_data_with_no_processors.latest_version
        expected = json.loads(expected_json)
        assert actual == expected


class TestResourceVariableBasicAnnotationProcessor:
    def test_process(self, hcl_convert_data_with_no_processors):
        resource_name = "demo_notebook_resource"
        data = {
            "path1": "world",
            "path2": {
                "nest2": "data"
            },
        }
        expected_json = """
                            {
                                "path2": {
                                    "@expr:nest2": "var.demo_notebook_resource_hcl-id_nest2"
                                },
                                "@expr:path1": "var.demo_notebook_resource_hcl-id_path1"
                            }     
        """
        hcl_convert_data_with_no_processors.modify_json(data)
        rvbap = ResourceVariableBasicAnnotationProcessor(resource_name, ["path1",
                                                                         "path2.nest2",
                                                                         ])
        rvbap.process(hcl_convert_data_with_no_processors)
        # print(json.dumps(hcl_convert_data_with_no_processors.latest_version, indent=4))
        assert hcl_convert_data_with_no_processors.latest_version == json.loads(expected_json)
        hcl = hcl_convert_data_with_no_processors.to_hcl(False)
        assert 'variable "demo_notebook_resource_hcl-id_path1"' in hcl
        assert 'variable "demo_notebook_resource_hcl-id_nest2"' in hcl
        assert 'resource "databricks_notebook" "hcl-id"' in hcl


class TestMappedGrokVariableBasicAnnotationProcessor:
    def test_process(self, hcl_convert_data_with_no_processors):
        resource_name = "demo_notebook_resource"
        data = {
            "s3_path": "s3a://testing_hello_world/test\ndemohaha\ns3a://my_bucket/hello_world",
            "path2": {
                "nest2": "s3a://nested_bucket/hello_world"
            },
            "path3": "testing/path/now"
        }
        hcl_convert_data_with_no_processors.modify_json(data)
        print(data)
        rvbap = MappedGrokVariableBasicAnnotationProcessor(resource_name,
                                                           {
                                                               "s3_path": "s3a://%{DATA:value}/%{GREEDYDATA}",
                                                               "path2.nest2": "s3a://%{DATA:value}/%{GREEDYDATA}",
                                                               "path3": None
                                                           })
        rvbap.process(hcl_convert_data_with_no_processors)
        # print(hcl_convert_data_with_no_processors.latest_version)
        print(hcl_convert_data_with_no_processors.to_hcl(True))
        print("\n".join([var.to_hcl(False) for var in hcl_convert_data_with_no_processors.mapped_variables]))
        # TODO: clean up test with comments and actions
        # print(json.dumps(hcl_convert_data_with_no_processors.latest_version, indent=4))
        # assert hcl_convert_data_with_no_processors.latest_version == json.loads(expected_json)
        # hcl = hcl_convert_data_with_no_processos.to_hcl(False)
        # assert 'variable "demo_notebook_resource_hcl-id_path1"' in hcl
        # assert 'variable "demo_notebook_resource_hcl-id_nest2"' in hcl
        # assert 'resource "databricks_notebook" "hcl-id"' in hcl
