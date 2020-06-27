import json
from jinja2 import Template

from core import AWSAttributes, template_string

jsonString="""
{
   "instance_pool_name":"On Demand Pools - All Users",
   "min_idle_instances":0,
   "aws_attributes":{
      "availability":"ON_DEMAND",
      "zone_id":"us-west-2c",
      "spot_bid_price_percent":100
   },
   "node_type_id":"i3.xlarge",
   "idle_instance_autotermination_minutes":60,
   "enable_elastic_disk":true,
   "preloaded_spark_versions":[
      "7.0.x-scala2.12"
   ],
   "instance_pool_id":"0623-201938-lows1-pool-x8dKEeez",
   "default_tags":{
      "Vendor":"Databricks",
      "DatabricksInstancePoolCreatorId":"100095",
      "DatabricksInstancePoolId":"0623-201938-lows1-pool-x8dKEeez"
   },
   "state":"ACTIVE",
   "stats":{
      "used_count":0,
      "idle_count":0,
      "pending_used_count":0,
      "pending_idle_count":0
   },
   "status":{

   }
}"""

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

        print(attribute_map['preloaded_spark_versions'])
        print(attribute_map['preloaded_spark_versions'][0].replace('\'',"\""))
        # REST returns an array for preloaded_spark_versions, need only the first cell
        print(type(attribute_map['preloaded_spark_versions']))
        print(type(attribute_map['preloaded_spark_versions'][0]))
        #attribute_map['preloaded_spark_versions'][0] = '{}'.format(', '.join(map('{}'.format, attribute_map['preloaded_spark_versions'])))
        print(attribute_map['preloaded_spark_versions'])
        print("[\"5.4.x-scala2.11\"]")

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

def test():
    instancePoolResp = json.loads(jsonString)
    pool = InstacePool(instancePoolResp)

    output_pool = PoolTFResource(instancePoolResp["instance_pool_id"], pool.resource, pool.blocks)
    print(output_pool.render())


test()
