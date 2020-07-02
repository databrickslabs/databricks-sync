import subprocess
import os,re
from export_example import export_pools


working_dir='./output/instance_pools/'

def exec(shell=False,*argv):

    if shell:
        process = subprocess.call(' '.join(argv[0][0:]),shell=True)
        return process,None

    else:
        print(' '.join(argv[0]))
        process = subprocess.run(argv[0],
                                     stdout=subprocess.PIPE,
                                     universal_newlines=True)
        process

    return process.returncode,process

def exec_print_output(only_error=False, shell=False, *argv):

    ret,process = exec(shell,argv)

    if only_error and ret != 0:
        print(process.stderr)
    elif process is not None:
        print(process.stdout)

def clean_product():
    exec_print_output(False,True,'rm', working_dir + "*")

clean_product()

export_pools(working_dir,False)

exec_print_output(False,False,'terraform', 'init', working_dir)

exec_print_output(False,False,'terraform', 'validate', working_dir)

#import:
import re, sys, os

tf_files = os.listdir(working_dir)
regex = r"#instance_pool_id = .*"

for file in tf_files:
    print("working on file:"+file)
    instance_pool_id = None
    for line in open(working_dir+file).readlines():
        if re.search(regex, line):
            instance_pool_id = line.split("=")[1].replace('"','').replace(' ','').replace('\n','')
            print(line)
            print(instance_pool_id)
    exec_print_output(False,False,'terraform', 'import',
                        '-config='+working_dir,
                        '-state='+working_dir+"/"+file+"state",
                        "databricks_instance_pool."+file.split('.')[0],
                        instance_pool_id)
    exec_print_output(False,False,'terraform', 'plan',
                          '-state=' + working_dir + "/" + file + "state",
                          working_dir)




