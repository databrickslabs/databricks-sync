import subprocess
import re, os
from pathlib import Path


from databricks_cli.jobs.api import JobsApi
from resources import *
from resources.core import genProvider, clean_product, exec_print_output


working_dir='./output/jobs/'
api_client = None

def export_jobs(output_dir,prt=False):
    global api_client
    if api_client is None:
        api_client = get_client()

    jobList = JobsApi(api_client).list_jobs()
    jobs = {}
    for jb in jobList['jobs']:
        print(jb)
        job = Job(jb)
        print (job.id)
        print(jb['settings']['name'])
        jobs[job.id] = job
        output_job = JobTFResource(jb["job_id"], job.resource, job.blocks)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        with open(output_dir + jb['settings']['name'].replace(' ', '_').replace('/', '_') +"_"+ str(jb["job_id"]) + '.tf',
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

#import:

os.environ['CLOUD_ENV'] = 'aws'
os.environ['TF_LOG'] = ''

tf_files = os.listdir(working_dir)
regex = r"#job_id = .*"

for file in tf_files:
    print("*****\n working on file:"+file+"\n****\n")
    job_id = None
    for line in open(working_dir+file).readlines():
        if re.search(regex, line):
            job_id = line.split("=")[1].replace('"', '').replace(' ', '').replace('\n', '')
            print(line)
            print(job_id)
    exec_print_output(False, False,'terraform', 'import',
                        '-config=' + working_dir,
                        '-state=' + working_dir + file +"state",
                        "databricks_job." + file.split('.')[0],
                      job_id)
    exec_print_output(False,False,'terraform', 'plan',
                            '-state=' + working_dir + file + "state",
                            '-out='+ working_dir + file + "plan",
                            working_dir)




