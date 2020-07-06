import re, os
from pathlib import Path


from databricks_cli.instance_pools.api import InstancePoolsApi
from resources.core import genProvider, clean_product, exec_print_output, get_client
from resources.instance_pool import InstacePool,PoolTFResource

def export_pools(output_dir,prt=False):
    global api_client
    if api_client is None:
        api_client = get_client()

    poolList = InstancePoolsApi(api_client).list_instance_pools()
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
api_client = None

clean_product(working_dir)

genProvider(working_dir)

export_pools(working_dir,False)

exec_print_output(False, False, 'terraform', 'init', working_dir)

exec_print_output(False, False, 'terraform', 'validate', working_dir)

tf_files = os.listdir(working_dir)
regex = r"#instance_pool_id = .*"

for file in tf_files:
    if file != 'provider.tf' :
        print("*****\n working on file:"+file+"\n****\n")
        instance_pool_id = None
        for line in open(working_dir+file).readlines():
            if re.search(regex, line):
                instance_pool_id = line.split("=")[1].replace('"','').replace(' ','').replace('\n','')
                print(line)
                print(instance_pool_id)
        exec_print_output(False, False, 'terraform', 'import',
                            '-config=' + working_dir,
                            '-state=' + working_dir +'terraform.state',
                            "databricks_instance_pool." + file.split('.')[0],
                          instance_pool_id)

exec_print_output(False, False, 'terraform', 'plan',
                        '-state=' + working_dir +'terraform.state',
                        '-out=' + working_dir + "terrafrom.tfplan",
                  working_dir)




