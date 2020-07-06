import json
from jinja2 import Template

from .core import AWSAttributes, template_string, genTFValidName, core_resource_blocks
from .cluster import ClusterTFResource

jsonString="""
{
    "job_id": 30631,
    "settings": {
        "name": "MLflow Quick Start Part 1: Training and Logging",
        "new_cluster": {
            "spark_version": "6.1.x-scala2.11",
            "aws_attributes": {
                "zone_id": "us-west-2c",
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK"
            },
            "node_type_id": "i3.xlarge",
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            "enable_elastic_disk": false,
            "num_workers": 8
        },
        "email_notifications": {},
        "timeout_seconds": 0,
        "notebook_task": {
            "notebook_path": "/Users/mahdi.askari@databricks.com/UDAP-Perth/MLflow Quick Start Part 1: Training and Logging"
        },
        "max_concurrent_runs": 1
    },
    "created_time": 1575587761202,
    "creator_user_name": "mahdi.askari@databricks.com"
}"""

#TODO can we preserve the created_time
#TODO Can we max_concurrent_runs to a variable (need 0 for DR) Can we add a new value to retain the old one?

#TODO need to cover all cases: 
    # Spark submit/JAR/Python/Notebook 
    # new cluster/existing 
    # others?


class JobTFResource:
    block_key_map = {
        "aws_attributes": AWSAttributes,
#        "new_cluster": NewCluster
    }
    ignore_attribute_key = {
        "created_time","creator_user_name"
    }
    comment_attributes_key = {
        "job_id"
    }

    def __init__(self, id, attribute_map, blocks):
        self.id = id
        self.template = Template(template_string)
        self.attribute_map = attribute_map
        self.blocks = blocks

    def render(self):
        resource_name=genTFValidName(self.blocks[0].attribute_map['name'])
        return self.template.render(resource_type="databricks_job", resource_name=resource_name, resource_id=self.id,
                                    attribute_map=self.attribute_map,comment_attributes=self.comment_attributes_key,
                                    blocks=[block.render() for block in self.blocks])
class Job:

    def __init__(self, json):
        self.id=json["job_id"]
        self.resource = {}
        self.blocks = []
        self.parse(json)

    def parse(self, json):

        # Need to eliminate the "settings" level
        json.update(json['settings'])
        del json['settings']

        json.update(json['notebook_task'])
        del json['notebook_task']

        for key in json.keys():
            # Catch all blocks
            print(key)
            if key in JobTFResource.block_key_map:
                self.blocks += [JobTFResource.block_key_map[key].parse(json[key])]
            elif key not in JobTFResource.ignore_attribute_key:
                assert type(json[key]) is not dict, "key is {key}".format(key=key)
                self.resource[key] = json[key]

def test():
    jobResp = json.loads(jsonString)
    job = Job(jobResp)

    output_job = JobTFResource(jobResp["job_id"], job.resource, job.blocks)
    print(output_job.render())


