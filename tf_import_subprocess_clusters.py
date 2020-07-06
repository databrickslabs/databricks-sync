import re, os
from pathlib import Path


from databricks_cli.clusters.api import ClusterApi
from resources.core import genProvider, clean_product, exec_print_output, get_client
from resources.cluster import Cluster,ClusterTFResource

def export_clusters(output_dir, prt=False):
    global api_client
    if api_client is None:
        api_client = get_client()

    clusterList = ClusterApi(api_client).list_clusters()
    clusters = {}
    for cl in clusterList['clusters']:
        if cl['cluster_source'] != 'JOB':
            print(cl)
            cluster = Cluster(cl)
            print (cluster.id)
            clusters[cluster.id] = cluster
            output_cluster = ClusterTFResource(cl["cluster_id"], cluster.resource, cluster.blocks)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            with open(output_dir + cl["cluster_name"].replace(' ', '_').replace('/', '_') +"_"+ cl["cluster_id"] + '.tf',
                      'w') as outfile:
                outfile.write(output_cluster.render())
                outfile.close()

    if prt:
        print(clusters)

working_dir='./output/clusters/'
api_client = None

clean_product(working_dir)

genProvider(working_dir)

export_clusters(working_dir, False)

exec_print_output(False, False, 'terraform', 'init', working_dir)

exec_print_output(False, False, 'terraform', 'validate', working_dir)

tf_files = os.listdir(working_dir)
regex = r"#cluster_id = .*"

for file in tf_files:
    if file != 'provider.tf' :
        print("*****\n working on file:"+file+"\n****\n")
        cluster_id = None
        for line in open(working_dir+file).readlines():
            if re.search(regex, line):
                cluster_id = line.split("=")[1].replace('"', '').replace(' ', '').replace('\n', '')
                print(line)
                print(cluster_id)
        exec_print_output(False, False, 'terraform', 'import',
                            '-config=' + working_dir,
                            '-state=' + working_dir +'terraform.state',
                            "databricks_cluster." + file[:-3].replace('.','_'),
                          cluster_id)

exec_print_output(False, False, 'terraform', 'plan',
                        '-state=' + working_dir +'terraform.state',
                        '-out=' + working_dir + "terrafrom.tfplan",
                  working_dir)




