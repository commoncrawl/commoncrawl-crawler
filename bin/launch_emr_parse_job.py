import getopt
import sys
import boto
import boto.emr
from boto.emr.step import JarStep
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
import time


usage_string = """
USAGE:

    launch_emr_parse_job.py --awsKey=[awsKey] --awsSecret=[awsSecret] --keypair=[key pair name] --core-count=[core count] --spot-count=[spot count] --spot-bid=[bid price]

Where:
	awsKey 		- the aws key
	awsSecret 	- the aws secret
	keypair    - the aws public/private key pair to use 
	core-count 	- the number of core instances to run 
	spot-count  - the number of spot instances to run 
	spot-bid	- spot instance bid price
"""

def usage():
    print usage_string
    sys.exit()

try:
	opts, args = getopt.getopt(sys.argv[1:],'',['awsKey=','awsSecret=','core-count=','spot-count=','spot-bid=','keypair=','test'])
except:
	usage()

# set your aws keys and S3 bucket, e.g. from environment or .boto

params = {'aws_key' : None,
          'secret' : None,
		  'keypair' : None,
          's3_bucket' : 'commoncrawl-emr',
		  'num_core' : 2,
		  'num_spot' : 0,
		  'spot_bid_price' : None,
		  'test_mode' : False
		 }

for o, a in opts:
	if o in ('--awsKey'):
		params['aws_key']=a
	if o in ('--awsSecret'):
		params['secret']=a
	if o in ('--core-count'):
		params['num_core'] =a
	if o in ('--spot-count'):
		params['num_spot']=a
	if o in ('--keypair'):
		params['keypair']=a
	if o in ('--spot-bid'):
		params['spot_bid_price']=a
	if o in ('--test'):
		params['test_mode']=True
	
required = ['aws_key','secret','keypair']

for pname in required:
    if not params.get(pname, None):
        print '\nERROR:%s is required' % pname
        usage()

for p, v in params.iteritems():
	print "param:" + `p`+ " value:" + `v`

conn = boto.connect_emr(params['aws_key'],params['secret'])

bootstrap_step1 = BootstrapAction("install_cc", "s3://commoncrawl-public/config64.sh",[params['aws_key'], params['secret']])
bootstrap_step2 = BootstrapAction("configure_maxtasktrackers", "s3://elasticmapreduce/bootstrap-actions/configure-hadoop",["-m","mapred.tasktracker.map.tasks.maximum=6"])
bootstrap_step3 = BootstrapAction("configure_jobtrackerheap", "s3://elasticmapreduce/bootstrap-actions/configure-daemons",["--jobtracker-heap-size=12096"])

namenode_instance_group = InstanceGroup(1,"MASTER","c1.xlarge","ON_DEMAND","MASTER_GROUP")
core_instance_group = InstanceGroup(params['num_core'],"CORE","c1.xlarge","ON_DEMAND","CORE_GROUP")

instance_groups=[]
if params['num_spot'] <= 0:
	instance_groups=[namenode_instance_group,core_instance_group]
else:
	
	if not params['spot_bid_price']:
		print '\nERROR:You must specify a spot bid price to use spot instances!'
		usage()
	
	spot_instance_group = InstanceGroup(params['num_spot'],"TASK","c1.xlarge","SPOT","INITIAL_TASK_GROUP",params['spot_bid_price'])
		
	instance_groups=[namenode_instance_group,core_instance_group,spot_instance_group]

args = []

if params['test_mode'] == True:
	args.append('--testMode')

step = JarStep(
	name="CCParseJob",
	jar="s3://commoncrawl-public/commoncrawl-0.1.jar",
	main_class="org.commoncrawl.mapred.ec2.parser.EC2Launcher",
	action_on_failure="CANCEL_AND_WAIT",
	step_args=args)
	
print  instance_groups

#	instance_groups=[namenode_instance_group,core_instance_group,spot_instance_group],
jobid = conn.run_jobflow(
	name="EMR Parser JOB", 
	availability_zone="us-east-1d",
    log_uri="s3://" + params['s3_bucket'] + "/logs", 
	ec2_keyname=params['keypair'],
	instance_groups=instance_groups,
	keep_alive=True,
    enable_debugging=True,
	hadoop_version="0.20.205",
	steps = [step],    
	bootstrap_actions=[bootstrap_step1,bootstrap_step2,bootstrap_step3],
	ami_version="2.0.4"
    )

print "finished spawning job (note: starting still takes time)"

state = conn.describe_jobflow(jobid).state
print "job state = ", state
print "job id = ", jobid
while state != u'COMPLETED':
    print time.localtime()
    time.sleep(30)
    state = conn.describe_jobflow(jobid).state
    print "job state = ", state
    print "job id = ", jobid

print "final output can be found in s3://" + params['s3_bucket'] + "/output" + TIMESTAMP
print "try: $ s3cmd sync s3://" + params['s3_bucket'] + "/output" + TIMESTAMP + " ."
