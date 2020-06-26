from core import AWSAttributes, template_string

#############################################
######  start instance_pool.py
#############################################
import json
from jinja2 import Template

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
# Left hand side is the cluster json resp
# Right hand side is the cluster terraform argument keys
# clusterResponseKeyToTFKeyMapping = {
#     "cluster_name": "cluster_name",
#     "spark_version": "spark_version",
#     "node_type_identity": "node_type_id",
#     "num_workers": "r_num_workers"
# }
cluster_resource_blocks = {
    # "autoscale": """
    # autoscale {
    #          min_workers = {{min_workers}}
    #          max_workers = {{max_workers}}
    #
    #     }
    # """,
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
            {%- if script.s3 %}s3 {
                    {%- for key, value in script.s3.items() %}
                    {{ key }} = "{{ value }}"
                    {%- endfor %}
           }{%- endif %}
    }
    {% endfor -%}
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
        return ClusterSparkEnvVars(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_env_vars",attributes=self.attribute_map)

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
        #self.template = Template(cluster_resource_blocks["autoscale"])
        self.template = Template(cluster_resource_blocks["flat_block"])
        assert "min_workers" in attribute_map
        assert "max_workers" in attribute_map
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        return ClusterAutoScaleBlock(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="autoscale", attributes=self.attribute_map)
        #return self.template.render(min_workers=self.attribute_map["min_workers"],
        #                            max_workers=self.attribute_map["max_workers"])


class ClusterTFResource:
    block_key_map = {
        "autoscale": ClusterAutoScaleBlock,
        "aws_attributes": AWSAttributes,
        "spark_conf": ClusterSparkConf,
        "init_scripts": ClusterInitScript,
        "custom_tags": ClusterCustomTags,
        "spark_env_vars": ClusterSparkEnvVars,
        "cluster_log_conf": ClusterLogConf,
        "docker_image": ClusterDockerImage
    }
    ignore_block_key = {
        "driver", "executors", "default_tags","cluster_log_status"
    }
    ignore_attribute_key = {
        "spark_context_id", "jdbc_port", "cluster_source", "state", "state_message", "start_time", "terminated_time",
        "last_state_loss_time", "last_activity_time", "cluster_memory_mb", "cluster_cores", "creator_user_name",
        "pinned_by_user_name", "init_scripts_safe_mode", "enable_local_disk_encryption", "cluster_id","termination_reason","policy_id"
    }

    def __init__(self, id, attribute_map, blocks):
        self.id = id
        self.template = Template(template_string)
        self.attribute_map = attribute_map
        self.blocks = blocks

    def render(self):
        resource_name=self.attribute_map['cluster_name'].replace(" ","_").replace('.','_').replace('/','_')
        return self.template.render(resource_type="databricks_cluster", resource_name=resource_name, resource_id=self.id,
                                    attribute_map=self.attribute_map,
                                    blocks=[block.render() for block in self.blocks])


class Cluster:

    def __init__(self, cluster_json):
        self.id = ""
        self.resource = {}
        self.blocks = []
        self.parse(cluster_json)

    def parse(self, cluster_json):
        for key in cluster_json.keys():
            print(key)
            # Catch all blocks
            if key in ClusterTFResource.block_key_map:
                # clusterResp[key] is the value in the json and the block_key map will point to the class to handle the block
                self.blocks += [ClusterTFResource.block_key_map[key].parse(cluster_json[key])]
            elif key not in ClusterTFResource.ignore_block_key and key not in ClusterTFResource.ignore_attribute_key:
                assert type(cluster_json[key]) is not dict, "key is {key}".format(key=key)
                self.resource[key] = cluster_json[key]


def test():
    clusterResp = json.loads(jsonString)
    cluster = Cluster(clusterResp)

    output_cluster = ClusterTFResource(clusterResp["cluster_id"], cluster.resource, cluster.blocks)
    print(output_cluster.render())

#############################################
######  end cluster.py
#############################################

#############################################
######  start instance_pool.py
#############################################
class PoolTFResource:
    block_key_map = {
        "aws_attributes": AWSAttributes,

    }
    ignore_block_key = {
        "stats"
    }
    ignore_attribute_key = {
        "state","status","default_tags","instance_pool_id"
    }

    def __init__(self, id, attribute_map, blocks):
        self.id = id
        self.template = Template(template_string)
        self.attribute_map = attribute_map
        self.blocks = blocks

    def render(self):
        resource_name=self.attribute_map['instance_pool_name'].replace(" ","_").replace('.','_').replace('/','_')
        return self.template.render(resource_type="databricks_instance_pool", resource_name=resource_name, resource_id=self.id,
                                    attribute_map=self.attribute_map,
                                    blocks=[block.render() for block in self.blocks])
class InstacePool:

    def __init__(self, pool_json):
        self.id = ""
        self.resource = {}
        self.blocks = []
        self.parse(pool_json)

    def parse(self, pool_json):
        for key in pool_json.keys():
            print(key)
            # Catch all blocks
            if key in PoolTFResource.block_key_map:
                # poolResp[key] is the value in the json and the block_key map will point to the class to handle the block
                self.blocks += [PoolTFResource.block_key_map[key].parse(pool_json[key])]
            elif key not in PoolTFResource.ignore_block_key and key not in PoolTFResource.ignore_attribute_key:
                assert type(pool_json[key]) is not dict, "key is {key}".format(key=key)
                self.resource[key] = pool_json[key]


#############################################
######  end instance_pool.py
#############################################
import json

from databricks_cli.sdk import ApiClient
#TODO Why do I have to fully describe this?
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.provider import get_config_for_profile

from jinja2 import Environment, FileSystemLoader

#from .cluster import Cluster

#TODO test SSH_PUBLIC_KEY
#TODO test cluster_log_conf with S3
#TODO test "init_scripts": with S3
#TODO "init_scripts":[{"dbfs":{"destination":"dbfs:/databricks/init_scripts/overwatch_proto.sh"}},{"s3":{"destination":"s3://aaa","region":""}},{"dbfs":{"destination":"dbfs:/3"}},{"s3":{"destination":"s3://4","region":""}}]
#TODO test init_script S3 with KMS
#TODO how do docker images work in Azure?
#TODO test single_user_name
#TODO test idempotency_token

# read DB config file and get a client

def writeTF(outputfile):
    file_loader = FileSystemLoader('../templates')
    env = Environment(loader=file_loader)

    template = env.get_template('clusters.jinja2')

    output = template.render(clusters=clusterList['clusters'])
    with open(outputfile, 'w') as outfile:
        outfile.write("provider \"databricks\" { \n }")
        outfile.write(output)
    print("provider \"databricks\" { \n }")
    print(output)

def writeJson(file="/tmp/clusters.json"):
    with open(file, 'w') as outfile:
        json.dump(clusterList, outfile)
    outfile.close()

def compareWithWorkspace(file="/tmp/clusters.json"):
    with open(file) as infile:
        #input = json.dumps(json.load(infile), sort_keys=True)
        input = json.load(infile)['clusters']
    infile.close()

    #workspace = json.dumps(clusterList, sort_keys=True)
    # filter out jobs cluster
    # modify to use "cluster_source":"JOB",
    workspace = {k: v for k, v in clusterList['clusters'].items() if not k.startswith('job-')}

    if not input == workspace:
        for i in range(len(input)):
            diffs = compareJson.compare(input[i],workspace[i],input[i]['cluster_id'])
            if len(diffs) > 0:
                print('\r\nFound differences comparing \n ',input[i],' file and \n ',workspace[i])
            for diff in diffs:
                print(diff['type'] + ': ' + diff['message'])

def provider():
    return """
                provider "databricks" { 
                }
             """


config = get_config_for_profile('demo')
api_client = ApiClient(host=config.host, token=config.token)
poolList = InstancePoolsApi(api_client).list_instance_pools()
print(type(poolList))
for pool in poolList['instance_pools']:
    print(pool)

OUTPUT_PATH= '../output/'

poolList = InstancePoolsApi(api_client).list_instance_pools()
for pl in poolList['instance_pools']:
    print(pl)
    pool = InstacePool(pl)
    output_pool = PoolTFResource(pl["instance_pool_id"], pool.resource, pool.blocks)
    with open(OUTPUT_PATH + pl["instance_pool_name"].replace(' ', '_').replace('/', '_') + pl["instance_pool_id"] + '.tf',
              'w') as outfile:
        outfile.write(output_pool.render())
        outfile.close()
    print(output_pool.render())

clusterList = ClusterApi(api_client).list_clusters()
with open(OUTPUT_PATH+'Provider.tf', 'w') as outfile:
    outfile.write(provider())
    outfile.close()

clusters = []
for cl in clusterList['clusters']:
    print(cl)
    print(cl['cluster_source'])
    if cl['cluster_source'] != "JOB":
        cluster = Cluster(cl)

        output_cluster = ClusterTFResource(cl["cluster_id"], cluster.resource, cluster.blocks)
        with open(OUTPUT_PATH+cl["cluster_name"].replace(' ', '_').replace('/','_') + cl["cluster_id"] + '.tf', 'w') as outfile:
            outfile.write(output_cluster.render())
            outfile.close()
        print(output_cluster.render())

    #clusters += cluster(cl)




#writeJson()
#print("compare with no change")
#compareWithWorkspace()


#clusterList['clusters'][0]['autotermination_minutes']=200
#print("compare with a change")
#compareWithWorkspace()

#writeTF("/Users/itaiweiss/databricks-terraform/terraform-provider-databricks/examples/import-db-test/main.tf")
