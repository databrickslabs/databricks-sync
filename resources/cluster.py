import json
from jinja2 import Template

from .core import AWSAttributes, template_string, core_resource_blocks, genTFValidName, get_client
from databricks_cli.libraries.api import LibrariesApi


jsonString = """
{
         "cluster_id":"0318-151752-abed99",
         "driver":{
            "public_dns":"",
            "node_id":"cd18c7bcd6ce41e9abfe1ea0b43590d6",
            "node_aws_attributes":{
               "is_spot":false
            },
            "instance_id":"i-00eb95eb6a35341ce",
            "start_timestamp":1592815837080,
            "host_private_ip":"10.0.233.145",
            "private_ip":"10.0.250.243"
         },
         "executors":[
            {
               "public_dns":"",
               "node_id":"5d024284dd93442aae5c41e3ec355c5b",
               "node_aws_attributes":{
                  "is_spot":false
               },
               "instance_id":"i-0ebef673cfb4b2efc",
               "start_timestamp":1592982646556,
               "host_private_ip":"10.0.252.39",
               "private_ip":"10.0.243.187"
            }
         ],
         "spark_context_id":1277495234349718182,
         "jdbc_port":10000,
         "cluster_name":"Shared Autoscaling",
         "spark_version":"6.6.x-cpu-ml-scala2.11",
         "spark_conf":{
            "spark.databricks.conda.condaMagic.enabled":"true"
         },
         "aws_attributes":{
            "zone_id":"us-west-2c",
            "first_on_demand":1,
            "availability":"SPOT_WITH_FALLBACK",
            "instance_profile_arn":"arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access",
            "spot_bid_price_percent":100,
            "ebs_volume_count":0
         },
         "node_type_id":"i3.2xlarge",
         "driver_node_type_id":"i3.4xlarge",
         "custom_tags":{
            "KeepAlive":"True"
         },
         "autotermination_minutes":240,
         "enable_elastic_disk":true,
         "cluster_source":"UI",
         "init_scripts":[
            {
               "dbfs":{
                  "destination":"dbfs:/databricks/init_scripts/overwatch_proto.sh"
               }
            },
            {
               "dbfs":{
                  "destination":"dbfs:/databricks/jupyter/kernel_gateway_init.sh"
               }
            }
         ],
         "enable_local_disk_encryption":false,
         "state":"RUNNING",
         "state_message":"",
         "start_time":1592371009193,
         "terminated_time":0,
         "last_state_loss_time":1592887081313,
         "last_activity_time":1592887055636,
         "autoscale":{
            "min_workers":1,
            "max_workers":14
         },
         "cluster_memory_mb":187392,
         "cluster_cores":24.0,
         "default_tags":{
            "Vendor":"Databricks",
            "Creator":"mwc@databricks.com",
            "ClusterName":"Shared Autoscaling",
            "ClusterId":"0318-151752-abed99"
         },
         "creator_user_name":"mwc@databricks.com",
         "pinned_by_user_name":"100095",
         "init_scripts_safe_mode":false
      }
"""

cluster_resource_blocks2 = core_resource_blocks
cluster_resource_blocks2["autoscale"] = {"""
    autoscale {
             min_workers = {{min_workers}}
             max_workers = {{max_workers}}
         }
     """}
cluster_resource_blocks2["init_scripts"] = {"""
 {% for script in init_scripts -%}
 init_scripts {
         {% if script.dbfs %}dbfs {
             destination = "{{ script.dbfs.destination }}"
         }{%- endif %}
         {% if script.s3 %}s3 ={
         destination = "{{ script.s3.destination }}"
         {% if script.s3.region %}region = "{{ script.s3.region }}"{%- endif -%}
         {% if script.s3.endpoint %}endpoint = "{{ script.s3.endpoint }}"{%- endif -%}
         {% if script.s3.enable_encryption %}enable_encryption = "{{ script.s3.enable_encryption }}"{%- endif -%}
         {% if script.s3.encryption_type %}encryption_type = "{{ script.s3.encryption_type }}"{%- endif -%}
         {% if script.s3.kms_key %}kms_key = "{{ script.s3.kms_key }}"{%- endif -%}
         {% if script.s3.canned_acl %}canned_acl = "{{ script.s3.canned_acl }}"{%- endif -%}
        }{%- endif %}
 }
 {% endfor %}
 """}

cluster_resource_blocks = {
    "autoscale": """
     autoscale {
              min_workers = {{min_workers}}
              max_workers = {{max_workers}}
         }
     """,
    "flat_map": """
     {{property_name}} ={
         {%- for key, value in attributes.items() %}
         "{{ key }}" = "{{ value }}"
         {%- endfor %}
     }
     """,
    "flat_block": """
     {{property_name}} {
         {%- for key, value in attributes.items() %}
         {{ key }} = "{{ value }}"
         {%- endfor %}
     }
     """,
    "init_scripts": """
     {% for script in init_scripts -%}
     init_scripts {
             {% if script.dbfs %}dbfs {
                 destination = "{{ script.dbfs.destination }}"
             }{%- endif %}
             {% if script.s3 %}s3 ={
             destination = "{{ script.s3.destination }}"
             {% if script.s3.region %}region = "{{ script.s3.region }}"{%- endif -%}
             {% if script.s3.endpoint %}endpoint = "{{ script.s3.endpoint }}"{%- endif -%}
             {% if script.s3.enable_encryption %}enable_encryption = "{{ script.s3.enable_encryption }}"{%- endif -%}
             {% if script.s3.encryption_type %}encryption_type = "{{ script.s3.encryption_type }}"{%- endif -%}
             {% if script.s3.kms_key %}kms_key = "{{ script.s3.kms_key }}"{%- endif -%}
             {% if script.s3.canned_acl %}canned_acl = "{{ script.s3.canned_acl }}"{%- endif -%}
            }{%- endif %}
     }
     {% endfor %}
     """,
    "custom_tags": """
     """,
    "2dim_block": """
{{property_name}} {
    {%- for key, value in attributes.items() %}
        {%- if value is not string %}
            {{ key }} {
                {%- for key2, value2 in value.items() %}
                {{ key2 }} = "{{ value2 }}"
                {%- endfor %}
            }
        {% else %}
            {{ key }} = "{{ value }}"
        {%- endif %}
    {%- endfor %}
}
"""
}

listOfLibs = ['maven' , 'cran', 'pypi', 'whl', 'jar', 'egg']

class LibraryDetails:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        for i in range(len(attribute_map)):
            for item in attribute_map[i]['library']:
                assert item in listOfLibs
                # JAR JSON is different, need to add a dict around it
                if item in ('jar','whl','egg'):
                    attribute_map[i]['library'] = { item: {
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
        self.template = Template(cluster_resource_blocks["2dim_block"])


    @staticmethod
    def parse(input_dictionary):
        return ClusterDockerImage(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="docker_image",attributes=self.attribute_map)

class ClusterLogConf:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(cluster_resource_blocks["2dim_block"])


    @staticmethod
    def parse(input_dictionary):
        return ClusterLogConf(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="cluster_log_conf",attributes=self.attribute_map)

class ClusterSparkEnvVars:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(cluster_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterCustomTags(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_env_vars", attributes=self.attribute_map)


class ClusterCustomTags:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(cluster_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterCustomTags(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="custom_tags", attributes=self.attribute_map)


class ClusterSparkConf:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(cluster_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return ClusterSparkConf(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_conf", attributes=self.attribute_map)


class ClusterInitScript:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(cluster_resource_blocks["init_scripts"])
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
        self.template = Template(cluster_resource_blocks["flat_block"])
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
    ignore_block_key = {
        "driver", "executors", "default_tags","cluster_log_status"
    }
    ignore_attribute_key = {
        "spark_context_id", "jdbc_port", "cluster_source", "state", "state_message", "start_time", "terminated_time",
        "last_state_loss_time", "last_activity_time", "cluster_memory_mb", "cluster_cores", "creator_user_name",
        "pinned_by_user_name", "init_scripts_safe_mode", "enable_local_disk_encryption","termination_reason"
    }
    comment_attributes_key = {
        "cluster_id"
    }
    def __init__(self, id, attribute_map, blocks):
        self.id = id
        self.template = Template(template_string)
        self.attribute_map = attribute_map
        self.blocks = blocks

    def render(self):
        resource_name=genTFValidName(self.attribute_map['cluster_name'])
        return self.template.render(resource_type="databricks_cluster", resource_name=resource_name, resource_id=self.id,
                                    attribute_map=self.attribute_map,comment_attributes=self.comment_attributes_key,
                                    blocks=[block.render() for block in self.blocks])

class Cluster:

    def __init__(self, json):
        self.id=json["cluster_id"]
        self.resource = {}
        self.blocks = []
        self.parse(json)
        self.add_cluster_libraries()

    def parse(self, json):
        for key in json.keys():
            # Catch all blocks
            if key in ClusterTFResource.block_key_map:
                # clusterResp[key] is the value in the json and the block_key map will point to the class to handle the block
                self.blocks += [ClusterTFResource.block_key_map[key].parse(json[key])]
            elif key not in ClusterTFResource.ignore_block_key and key not in ClusterTFResource.ignore_attribute_key:
                assert type(json[key]) is not dict, "key is {key}".format(key=key)
                self.resource[key] = json[key]

    def add_cluster_libraries(self):
        lib_list = LibrariesApi(get_client()).cluster_status(self.id)
        if 'library_statuses' in lib_list:
            self.blocks += [ClusterTFResource.block_key_map["library"].parse(lib_list['library_statuses'])]

def test():
    clusterResp = json.loads(jsonString)
    cluster = Cluster(clusterResp)

    output_cluster = ClusterTFResource(clusterResp["cluster_id"], cluster.resource, cluster.blocks)
    print(output_cluster.render())


