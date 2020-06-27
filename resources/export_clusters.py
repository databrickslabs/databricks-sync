import json
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


from databricks_cli.sdk import ApiClient
#TODO Why do I have to fully describe this?
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.provider import get_config_for_profile

from instance_pool import InstacePool,PoolTFResource
from cluster import Cluster,ClusterTFResource

#from .resources import Cluster

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
    # filter out jobs resources
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
    Path(OUTPUT_PATH + '/instance_pools').mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH + '/instance_pools/' + pl["instance_pool_name"].replace(' ', '_').replace('/', '_') + pl["instance_pool_id"] + '.tf',
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
        Path(OUTPUT_PATH + '/clusters').mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_PATH+ '/clusters/' + cl["cluster_name"].replace(' ', '_').replace('/','_') + cl["cluster_id"] + '.tf', 'w') as outfile:
            outfile.write(output_cluster.render())
            outfile.close()
        print(output_cluster.render())

    #clusters += resources(cl)




#writeJson()
#print("compare with no change")
#compareWithWorkspace()


#clusterList['clusters'][0]['autotermination_minutes']=200
#print("compare with a change")
#compareWithWorkspace()

#writeTF("/Users/itaiweiss/databricks-terraform/terraform-provider-databricks/examples/import-db-test/main.tf")
