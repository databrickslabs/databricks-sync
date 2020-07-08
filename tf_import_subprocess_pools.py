import re, glob, os
from pathlib import Path


from databricks_cli.instance_pools.api import InstancePoolsApi
from resources.core import genProvider, clean_product, exec_print_output, get_client, genTFValidName
from resources.instance_pool import InstacePool,PoolTFResource

def export_pools(output_dir,prt=False):
    source_profile = 'demo'
    source_api_client = get_client(source_profile)

    poolList = InstancePoolsApi(source_api_client).list_instance_pools()
    pools = {}
    for pl in poolList['instance_pools']:
        print(pl)
        pool = InstacePool(pl)
        print (pool.id)
        pools[pool.id] = pool
        output_pool = PoolTFResource(pl["instance_pool_id"], pool.resource, pool.blocks)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        with open(output_dir + pl["instance_pool_name"].replace(' ', '_').replace('/', '_') +"_"+ pl["instance_pool_id"] + '.tf',
                  'w') as outfile:
            outfile.write(output_pool.render())
            outfile.close()

    if prt:
        print(pools)

working_dir='./output/instance_pools/'
source_api_client = None

clean_product(working_dir)

genProvider(working_dir)

export_pools(working_dir,False)

exec_print_output(False, False, 'terraform', 'init', working_dir)

exec_print_output(False, False, 'terraform', 'validate', working_dir)

tf_files = files = glob.glob(working_dir+"*.tf")
regex = r"#instance_pool_id = .*"

for file in tf_files:
    if os.path.basename(file) != 'provider.tf':
        print("*****\n working on file:"+file+"\n****\n")
        instance_pool_id = None
        for line in open(file).readlines():
            if re.search(regex, line):
                instance_pool_id = line.split("=")[1].replace('"','').replace(' ','').replace('\n','')
                print(line)
                print(instance_pool_id)
        exec_print_output(False, False, 'terraform', 'import',
                            '-config=' + working_dir,
                            '-state=' + working_dir +'terraform.state',
                            "databricks_instance_pool." + genTFValidName(os.path.basename(file)[:-3]),
                          instance_pool_id)

exec_print_output(False, False, 'terraform', 'plan',
                        '-state=' + working_dir +'terraform.state',
                        '-out=' + working_dir + "terrafrom.tfplan",
                  working_dir)




