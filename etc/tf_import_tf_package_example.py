from pprint import pprint

from python_terraform import *

working_dir='./output/instance_pools'

tf = Terraform(working_dir=working_dir)

tf.init(dir_or_plan=working_dir)

ret, out, err = tf.import_cmd('databricks_instance_pool.All_Spot_Pool_-_All_Users_0623-202012-velds2-pool-Vz8TJRfi', '0623-202012-velds2-pool-Vz8TJRfi"',no_color=IsFlagged)
pprint(err)
#tf.fmt(diff=True)


#tf.plan()
