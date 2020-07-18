import json
from jinja2 import Template

from .core import AWSAttributes, template_string, genTFValidName, core_resource_blocks
from .base_cluster import ClusterAutoScaleBlock, ClusterSparkConf, ClusterLogConf, ClusterSparkEnvVars, ClusterInitScript, ClusterCustomTags, ClusterDockerImage, LibraryDetails

jsonString="""
{
   "job_id":35306,
   "settings":{
      "name":"Training Pipeline",
      "new_cluster":{
         "spark_version":"6.6.x-cpu-ml-scala2.11",
         "aws_attributes":{
            "zone_id":"us-west-2c",
            "first_on_demand":0,
            "availability":"SPOT"
         },
         "node_type_id":"i3.xlarge",
         "enable_elastic_disk":False,
         "num_workers":2
      },
      "libraries":[
         {
            "pypi":{
               "package":"mlflow==1.8.0"
            }
         },
         {
            "pypi":{
               "package":"python-dotenv==0.10.3"
            }
         },
         {
            "pypi":{
               "package":"scikit-learn==0.23.1"
            }
         },
         {
            "whl":"dbfs:/databricks/mlflow/7022571/a02d7c62a3e5476e9ad1a364eacbe594/artifacts/dist/lendingclub_scoring-0.1.0-py3-none-any.whl"
         }
      ],
      "email_notifications":{

      },
      "timeout_seconds":100000000,
      "max_retries":1,
      "min_retry_interval_millis":0,
      "retry_on_timeout":False,
      "spark_python_task":{
         "python_file":"dbfs:/databricks/mlflow/7022571/a02d7c62a3e5476e9ad1a364eacbe594/artifacts/job/pipelines/training_pipeline/pipeline_runner.py",
         "parameters":[
            "dbfs:/databricks/mlflow/7022571/a02d7c62a3e5476e9ad1a364eacbe594/artifacts/job/pipelines/training_pipeline"
         ]
      },
      "max_concurrent_runs":1
   },
   "created_time":1591221636548,
   "creator_user_name":"michael.shtelma@databricks.com"
}"""

#TODO can we preserve the created_time
#TODO Can we max_concurrent_runs to a variable (need 0 for DR) Can we add a new value to retain the old one?

#TODO need to cover all cases: 
    # Spark submit/JAR/Python/Notebook 
    # new cluster/existing 
    # others?
class NewCluster:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["complex_struct"])
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        new_cluster_resource = {}
        new_cluster_blocks = []
        for key in input_dictionary:
            # Catch all blocks
            print(key)
            if key in JobTFResource.block_key_map:
                new_cluster_blocks += [JobTFResource.block_key_map[key].parse(input_dictionary[key])]
            elif key not in JobTFResource.ignore_attribute_key:
                assert type(input_dictionary[key]) is not dict, "key is {key}".format(key=key)
                new_cluster_resource[key] = input_dictionary[key]

        new_cluster_blocks.append(new_cluster_resource)
        return NewCluster(new_cluster_blocks, None)

    def render(self):
        return self.template.render(resource_name="new_cluster",
                                    attribute_map=self.attribute_map['resource'],
                                    blocks=[block.render() for block in self.blocks])

class NotebookBaseParameters:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["flat_map"])
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        print(input_dictionary)
        for key,value in input_dictionary.items():
            input_dictionary[key] = value.replace('"', r'\"')

        return NotebookBaseParameters(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="notebook_base_parameters", attributes=self.attribute_map)


class PythonTask:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_block"])

    @staticmethod
    def parse(input_dictionary):
        return PythonTask(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_python_task", attributes=self.attribute_map)

class SparkJarTask:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_block"])

    @staticmethod
    def parse(input_dictionary):
        # if 'parameters' in input_dictionary:
        #     new_list = []
        #     for item in input_dictionary['parameters']:
        #         new_list.append(item.replace('"','\"'))
        #     input_dictionary['parameters'] = new_list
            # for item in input_dictionary['parameters']:
            #     if isinstance(item, str):
            #         item = item.replace('"','\"')

        return SparkJarTask(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="spark_jar_task", attributes=self.attribute_map)

class JobLibrary:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_block_in_array"])

    @staticmethod
    def parse(input_dictionary):
        print(input_dictionary)
        temp_dict = {}
        for key in input_dictionary[0]:
            temp_dict['library_'+key] = {'path': input_dictionary[0][key],}

        return JobLibrary(temp_dict, None)

    def render(self):
        return self.template.render(property_name="spark_jar_task", attributes=self.attribute_map)

class JobSchedule:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_block"])

    @staticmethod
    def parse(input_dictionary):
        return JobSchedule(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="schedule", attributes=self.attribute_map)

class JobTask:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.blocks = blocks
        self.template = Template(core_resource_blocks["flat_map"])

    @staticmethod
    def parse(input_dictionary):
        return JobTask(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="notebook_task", attributes=self.attribute_map)

class JobEmailNotification:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["nested_array"])

        #remove redundand attributes
        if 'alert_on_last_attempt' in attribute_map:
            del attribute_map['alert_on_last_attempt']

        listOfoptions = ['on_start', 'on_success', 'on_failure', 'no_alert_for_skipped_runs']
        for item in attribute_map:
            assert item in listOfoptions
        self.blocks = blocks

    @staticmethod
    def parse(input_array):
        return JobEmailNotification(input_array, None)

    def render(self):
        return self.template.render(init_scripts=self.attribute_map)


class JobTFResource:
    block_key_map = {
        "autoscale": ClusterAutoScaleBlock,
        "aws_attributes": AWSAttributes,
        "spark_conf": ClusterSparkConf,
        "init_scripts": ClusterInitScript,
        "custom_tags": ClusterCustomTags,
        "spark_env_vars": ClusterSparkEnvVars,
        "cluster_log_conf": ClusterLogConf,
        "docker_image": ClusterDockerImage,
        "libraries": JobLibrary,
        "email_notifications": JobEmailNotification,
        "notebook_task": JobTask,
        "spark_submit_task": JobTask,
        "spark_python_task": PythonTask,
        "spark_jar_task": SparkJarTask,
        "schedule": JobSchedule,
        "base_parameters": NotebookBaseParameters,
        "new_cluster": NewCluster
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
        resource_name=genTFValidName(self.attribute_map['name'])
        return self.template.render(resource_type="databricks_job", resource_name=resource_name, resource_id=self.id,
                                    attribute_map=self.attribute_map,comment_attributes=self.comment_attributes_key,
                                    blocks=[block.render() for block in self.blocks])
class Job:

    def __init__(self, json, source_target_cluster_map):
        self.id=json["job_id"]
        self.resource = {}
        self.blocks = []
        self.valid = True
        if "existing_cluster_id" in json['settings']:
            if json['settings']['existing_cluster_id'] in source_target_cluster_map:
                self.target_cluster_id=source_target_cluster_map[json['settings']['existing_cluster_id']]
            else:
                self.valid = False
        else:
            self.target_cluster_id = None

        self.parse(json)

    def parse(self, json):

        # Need to eliminate the "settings" level
        if 'settings' in json:
            json.update(json['settings'])
            del json['settings']

        if 'notebook_task' in json:
            json.update(json['notebook_task'])
            json.pop('notebook_task')

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


