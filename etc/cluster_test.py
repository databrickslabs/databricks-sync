import json

from databricks_cli.sdk import ApiClient
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.provider import get_config_for_profile

from jinja2 import Environment, FileSystemLoader

from util import *

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
    # filter out jobs databricks_terraformer
    # modify to use "cluster_source":"JOB",
    workspace = {k: v for k, v in clusterList['clusters'].items() if not k.startswith('job-')}

    if not input == workspace:
        for i in range(len(input)):
            diffs = compareJson.compare(input[i],workspace[i],input[i]['cluster_id'])
            if len(diffs) > 0:
                print('\r\nFound differences comparing \n ',input[i],' file and \n ',workspace[i])
            for diff in diffs:
                print(diff['type'] + ': ' + diff['message'])




config = get_config_for_profile('demo')
source_api_client = ApiClient(host=config.host, token=config.token)
clusterList = ClusterApi(source_api_client).list_clusters()


#writeJson()
#print("compare with no change")
#compareWithWorkspace()


#clusterList['clusters'][0]['autotermination_minutes']=200
#print("compare with a change")
#compareWithWorkspace()

writeTF("/Users/itaiweiss/databricks-terraform/terraform-provider-databricks/examples/import-db-test/main.tf")
