import copy

from databricks_terraformer.sdk.message import Variable, ErrorMixin
from tests.sdk import *


class TestAPIData:

    def test_artifacts(self, sample_api_data_without_artifacts, sample_api_data_with_artifacts):
        assert sample_api_data_without_artifacts.artifacts == []
        assert sample_api_data_with_artifacts.artifacts == artifacts
        assert len(sample_api_data_with_artifacts.artifacts) == 1
        assert sample_api_data_with_artifacts.artifacts[0].get_content() == "hello mocked content"

    def test_workspace_url(self, sample_api_data_without_artifacts):
        assert sample_api_data_without_artifacts.workspace_url == workspace_url

    def test_raw_identifier(self, sample_api_data_without_artifacts):
        assert sample_api_data_without_artifacts.raw_identifier == raw_id

    def test_hcl_resource_identifier(self, sample_api_data_without_artifacts):
        assert sample_api_data_without_artifacts.hcl_resource_identifier == hcl_resource_identifier

    def test_data(self, sample_api_data_without_artifacts):
        assert sample_api_data_without_artifacts.data == data

    def test_local_save_path(self, sample_api_data_without_artifacts):
        assert sample_api_data_without_artifacts.local_save_path == local_save_path


def normalize_multiline(val):
    return "\n".join([line.lstrip(" ") for line in val.split("\n")])


class TestVariable:
    my_dummy_variable = "varname"
    value_default = "hello world"
    dummy_variable = Variable(my_dummy_variable, value_default)
    expected_hcl = """variable "varname" {
                      default = "hello world"
                    }"""

    def test_to_hcl(self):
        assert normalize_multiline(self.expected_hcl) in normalize_multiline(self.dummy_variable.to_hcl(False))


class TestHCLConvertData:
    def test_local_save_path(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        assert hcl_convert_data_with_no_processors.local_save_path == sample_api_data_with_artifacts.local_save_path

    def test_resource_name(self, hcl_convert_data_with_no_processors):
        assert hcl_convert_data_with_no_processors.folder_name == resource_name

    def test_artifacts(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        assert hcl_convert_data_with_no_processors.artifacts == sample_api_data_with_artifacts.artifacts

    def test_processors(self, hcl_convert_data_with_no_processors):
        assert hcl_convert_data_with_no_processors.map_processors == []

    def test_hcl_resource_identifier(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        assert hcl_convert_data_with_no_processors.hcl_resource_identifier == sample_api_data_with_artifacts.hcl_resource_identifier

    def test_latest_version(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        assert hcl_convert_data_with_no_processors.latest_version == sample_api_data_with_artifacts.data

    def test_lineage(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        assert hcl_convert_data_with_no_processors.lineage == [sample_api_data_with_artifacts.data]

    def test_mapped_variables(self, hcl_convert_data_with_no_processors):
        assert hcl_convert_data_with_no_processors.mapped_variables == []

    def test_resource_variables(self, hcl_convert_data_with_no_processors):
        assert hcl_convert_data_with_no_processors.resource_variables == []

    def test_modify_json(self, hcl_convert_data_with_no_processors, sample_api_data_with_artifacts):
        new_data = {"new": "lineage"}
        hcl_convert_data_with_no_processors.modify_json(new_data)
        assert hcl_convert_data_with_no_processors.latest_version == new_data
        assert hcl_convert_data_with_no_processors.lineage == [sample_api_data_with_artifacts.data, new_data]

    def test_add_mapped_variable(self, hcl_convert_data_with_no_processors):
        # pass
        variable_name = "test-var"
        default_value = "default-val"
        var = Variable(variable_name, default_value)
        hcl_convert_data_with_no_processors.add_mapped_variable(variable_name, default_value)
        assert len(hcl_convert_data_with_no_processors.mapped_variables) == 1
        assert hcl_convert_data_with_no_processors.mapped_variables[0] == var

    def test_add_resource_variable(self, hcl_convert_data_with_no_processors):
        variable_name = "test-var"
        default_value = "default-val"
        var = Variable(variable_name, default_value)
        hcl_convert_data_with_no_processors.add_resource_variable(variable_name, default_value)
        assert len(hcl_convert_data_with_no_processors.resource_variables) == 1
        assert hcl_convert_data_with_no_processors.resource_variables[0] == var

    def test_to_hcl(self, hcl_convert_data_with_no_processors):
        variable_name = "test-var"
        default_value = "default-val"
        hcl = hcl_convert_data_with_no_processors.to_hcl(False)
        assert f"Source Workspace Url: {workspace_url}" in hcl
        assert f'resource "{resource_name}" "{hcl_resource_identifier}"' in hcl
        assert f'test = "data"' in hcl

        hcl_convert_data_with_no_processors.add_resource_variable(variable_name, default_value)
        hcl = hcl_convert_data_with_no_processors.to_hcl(False)
        assert f'variable "{variable_name}"' in hcl
        assert f'default = "{default_value}"' in hcl
        assert f"Source Workspace Url: {workspace_url}" in hcl
        assert f'resource "{resource_name}" "{hcl_resource_identifier}"' in hcl
        assert f'test = "data"' in hcl


fake_error = ValueError("Bad value")


@ErrorMixin.manage_error
def fake_error_mixin(d: HCLConvertData):
    assert d is not None
    raise fake_error


@ErrorMixin.manage_error
def no_error_mixin(d: HCLConvertData):
    assert d is not None
    return d


@ErrorMixin.manage_error
def not_an_error_mixin(d: str):
    return d + "world"


class TestErrorMixin:
    def test_manage_error_with_error(self, hcl_convert_data_with_no_processors):
        # TODO: change all of these deeopcopies to fixtures
        fake_error_mixin(hcl_convert_data_with_no_processors)
        assert len(hcl_convert_data_with_no_processors.errors) == 1
        assert hcl_convert_data_with_no_processors.errors[0] == fake_error
        # Subsequent processing should be skipped as there is an error and should just
        # return the data object back
        fake_error_mixin(hcl_convert_data_with_no_processors)
        assert len(hcl_convert_data_with_no_processors.errors) == 1
        assert hcl_convert_data_with_no_processors.errors[0] == fake_error

    def test_manage_error_no_error(self, hcl_convert_data_with_no_processors):
        # TODO: change all of these deeopcopies to fixtures
        no_error_mixin(hcl_convert_data_with_no_processors)
        assert len(hcl_convert_data_with_no_processors.errors) == 0

    def test_manage_error_not_an_error_mixin(self):
        # TODO: change all of these deeopcopies to fixtures
        val = not_an_error_mixin("hello ")
        assert val == "hello world"
