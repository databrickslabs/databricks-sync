from jinja2 import Template

from resources.core import core_resource_blocks, AWSAttributes, template_string, genTFValidName

class LibraryDetails:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        listOfLibs = ['maven', 'cran', 'pypi', 'whl', 'jar', 'egg']

        for i in range(len(attribute_map)):
            for item in attribute_map[i]['library']:
                assert item in listOfLibs
                # JAR JSON is different, need to add a dict around it
                if item in ('jar', 'whl', 'egg'):
                    attribute_map[i]['library'] = {item: {
                        'path': attribute_map[i]['library'][item]
                    }
                    }

            if 'status' in attribute_map[i]:
                del attribute_map[i]['status']
            if 'is_library_for_all_clusters' in attribute_map[i]:
                del attribute_map[i]['is_library_for_all_clusters']
            if 'messages' in attribute_map[i]:
                del attribute_map[i]['messages']


        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_block_in_array"])

    @staticmethod
    def parse(input_dictionary):
        return LibraryDetails(input_dictionary, None)

    def render(self):
        return self.template.render( array=self.attribute_map)


class ClusterDockerImage:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        assert "url" in attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["2dim_block"])


    @staticmethod
    def parse(input_dictionary):
        return ClusterDockerImage(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="docker_image",attributes=self.attribute_map)


class ClusterLogConf:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["2dim_block"])


    @staticmethod
    def parse(input_dictionary):
        return ClusterLogConf(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="cluster_log_conf",attributes=self.attribute_map)


class ClusterSparkEnvVars:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterSparkEnvVars(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_env_vars", attributes=self.attribute_map)


class ClusterCustomTags:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterCustomTags(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="custom_tags", attributes=self.attribute_map)


class ClusterSparkConf:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterSparkConf(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_conf", attributes=self.attribute_map)


class ClusterInitScript:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["nested_array"])
        for item in attribute_map:
            assert (("dbfs" in item) or ("s3" in item))
        self.blocks = blocks

    @staticmethod
    def parse(input_array):
        return ClusterInitScript(input_array, None)

    def render(self):
        return self.template.render(init_scripts=self.attribute_map)


class ClusterAutoScaleBlock:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["flat_block"])
        assert "min_workers" in attribute_map
        assert "max_workers" in attribute_map
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        return ClusterAutoScaleBlock(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="autoscale", attributes=self.attribute_map)


class ClusterTFResource:
    block_key_map = {
        "autoscale": ClusterAutoScaleBlock,
        "aws_attributes": AWSAttributes,
        "spark_conf": ClusterSparkConf,
        "init_scripts": ClusterInitScript,
        "custom_tags": ClusterCustomTags,
        "spark_env_vars": ClusterSparkEnvVars,
        "cluster_log_conf": ClusterLogConf,
        "docker_image": ClusterDockerImage,
        "library": LibraryDetails
    }
    ignore_attribute_key = {
        "driver", "executors", "default_tags","cluster_log_status",
        "spark_context_id", "jdbc_port", "cluster_source", "state", "state_message", "start_time", "terminated_time",
        "last_state_loss_time", "last_activity_time", "cluster_memory_mb", "cluster_cores", "creator_user_name",
        "pinned_by_user_name", "init_scripts_safe_mode", "enable_local_disk_encryption","termination_reason"
    }
    #TODO implement pocliy
    comment_attributes_key = {
        "cluster_id","policy_id"
    }
    def __init__(self, id, attribute_map, blocks):
        self.id = id
        self.template = Template(template_string)
        self.attribute_map = attribute_map
        self.blocks = blocks

    def render(self):
        resource_name=genTFValidName(self.attribute_map['cluster_name'])
        resource_id=genTFValidName(self.id)
        return self.template.render(resource_type="databricks_cluster", resource_name=resource_name, resource_id=resource_id,
                                    attribute_map=self.attribute_map,comment_attributes=self.comment_attributes_key,
                                    blocks=[block.render() for block in self.blocks])