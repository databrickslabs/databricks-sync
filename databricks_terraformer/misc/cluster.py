import json

from .base_cluster import ClusterTFResource
from .core import core_resource_blocks, get_client
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


class Cluster:

    def __init__(self, json):
        self.id=json["cluster_id"]
        self.resource = {}
        self.blocks = []
        self.parse(json)
        self.add_cluster_libraries()

    def parse(self, json):
        if 'instance_pool_id' in json:
            json.pop('node_type_id')
            json.pop('driver_node_type_id')
            json.pop('aws_attributes')

        for key in json.keys():
            # Catch all blocks
            if key in ClusterTFResource.block_key_map:
                # clusterResp[key] is the value in the json and the block_key map will point to the class to handle the block
                self.blocks += [ClusterTFResource.block_key_map[key].parse(json[key])]
            elif key not in ClusterTFResource.ignore_attribute_key:
                assert type(json[key]) is not dict, "key is {key}".format(key=key)
                self.resource[key] = json[key]

    def add_cluster_libraries(self):
        lib_list = LibrariesApi(get_client()).cluster_status(self.id)
        if 'library_statuses' in lib_list:
            self.blocks += [ClusterTFResource.block_key_map["library"].parse(lib_list['library_statuses'])]

    def render(self):
        return ClusterTFResource(self["cluster_id"], self.resource, self.blocks)

def test():
    clusterResp = json.loads(jsonString)
    cluster = Cluster(clusterResp)

    output_cluster = ClusterTFResource(clusterResp["cluster_id"], cluster.resource, cluster.blocks)
    print(output_cluster.render())


