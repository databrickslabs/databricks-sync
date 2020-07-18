import re, glob, os
from pathlib import Path

from databricks_cli.jobs.api import JobsApi
from databricks_cli.clusters.api import ClusterApi

from databricks_terraformer import *
from databricks_terraformer.core import genProvider, clean_product, exec_print_output, genTFValidName


working_dir='./output/jobs/'

def cluster_source_target_mapping(source_api_client,target_api_client):
    source_cluster_list = ClusterApi(source_api_client).list_clusters()
    target_cluster_list = ClusterApi(target_api_client).list_clusters()
    source_clusters = {}
    for cl in source_cluster_list['clusters']:
        source_clusters[cl['cluster_name']] = cl['cluster_id']
    map = {}
    for cl in target_cluster_list['clusters']:
        map[cl['cluster_id']] = {'target_id': source_clusters[cl['cluster_name']],
                                    'name':cl['cluster_name']}

    return map

def export_jobs(output_dir,prt=False):
    source_profile = 'demo'
    target_profile = 'demo'
    source_api_client = get_client(source_profile)
    target_api_client = get_client(target_profile)

    jobList = JobsApi(source_api_client).list_jobs()

    source_target_cluster_map = cluster_source_target_mapping(source_api_client,target_api_client)
    jobs = {}
    for jb in jobList['jobs']:
        print(jb)
        job = Job(jb,source_target_cluster_map)
        print (job.id)
        print(jb['name'])
        jobs[job.id] = job
        output_job = JobTFResource(jb['job_id'], job.resource, job.blocks)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        with open(output_dir + jb['name'].replace(' ', '_').replace('/', '_') +"_"+ str(jb["job_id"]) + '.tf',
                  'w') as outfile:
            outfile.write(output_job.render())
            outfile.close()
        #print(output_job.render())

    if prt:
        print(jobs)



clean_product(working_dir)

genProvider(working_dir)

export_jobs(working_dir,False)

exec_print_output(False,False,'terraform', 'init', working_dir)

exec_print_output(False,False,'terraform', 'validate', working_dir)



tf_files = files = glob.glob(working_dir+"*.tf")
regex = r"#job_id = .*"

for file in tf_files:
    if os.path.basename(file) != 'provider.tf':
        print("*****\n working on file:"+file+"\n****\n")
        job_id = None
        for line in open(file).readlines():
            if re.search(regex, line):
                job_id = line.split("=")[1].replace('"', '').replace(' ', '').replace('\n', '')
                print(line)
                print(job_id)
        exec_print_output(False, False,'terraform', 'import',
                            '-config=' + working_dir,
                            '-state=' + working_dir + file +"state",
                            "databricks_job." + genTFValidName(os.path.basename(file)[:-3]),
                          job_id)

exec_print_output(False,False,'terraform', 'plan',
                        '-state=' + working_dir + file + "state",
                        '-out='+ working_dir + file + "plan",
                        working_dir)




