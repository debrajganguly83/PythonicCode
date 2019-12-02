#!/usr/bin/env python3

'''
    Script Name : dms_tasks_config.py
    Author : Debraj Ganguly
    Purpose :This script has reference parameters to be used by main script auto_dms_tasks.py. The parameters are configurable
            by the user to be able to run the main script for various purposes
    Dependencies : auto_dms_tasks.py
'''

import datetime
import os

# Variables for setting up the environment for Python in conjunction with UNIX environment
unix_home = os.path.expanduser("~")
script_home = os.path.join(unix_home, 'scripts')
log_home = os.path.join(unix_home, 'logs')
aws_region = 'us-east-1'

# Non-configurable Script Variables for auto_dms_tasks.py
dms_log_file_name = 'auto_dms_tasks_{}.log'.format(datetime.datetime.now().strftime("%y-%m-%d-%H-%M-%S"))
dms_op_log_filepath = os.path.join(log_home, dms_log_file_name)
table_mappings_file_name = 'table-mappings.json'
rep_task_set_filename = 'task-settings.json'

###########################################################################################################################

# Variables which are configurable

replication_instance_id = <Replication Instance Identifier>
log_group = "dms-tasks-" + replication_instance_id
replication_instance_arn = <Replication Instance ARN> # Parameter stating the replication instance

replicationtaskid_prefix = <Prefix Name>
replication_task_filter = 'replication-instance-arn'  # Parameter to identify what filter to use to extract the tasks
max_records = 100                                     # Parameter to determine how many response can be extracted in one go
use_arn_db_transforms = 'N'      # Parameter to determine whether the transform will be based on ARNs or identifiers
use_specific_tasks = 'Y'
change_replication_instance = 'N'

if change_replication_instance == 'Y':
    new_replication_inst_arn = ''

endpoint_type = 'endpoint-type'  # Parameter to identify the filter used to extract the endpoints
endpoint_type_val = 'Target'     # Parameter to identify the value for the filter
enable_logging = True            # Parameter to enable Cloudwatch logging

###############################################################################################################################

if use_arn_db_transforms == 'Y':
    src_endpoint_transforms = {
        <Old Source Endpoint ARN1> : <New Source Endpoint ARN1>,
        <Old Source Endpoint ARN2> : <New Source Endpoint ARN2>
    }

    tgt_endpoint_transforms = {
        <Old Target Endpoint ARN> : <New Target Endpoint ARN>
    }

else:
    src_endpoint_transforms = {
        <Old Source Endpoint Identifier1> : <New Source Endpoint Identifier1>,
        <Old Source Endpoint Identifier2> : <New Source Endpoint Identifier2>
    }

    tgt_endpoint_transforms = {
        <Old Target Endpoint Identifier> : <New Target Endpoint Identifier>
    }

if use_specific_tasks == 'Y':
    task_names = [<task name 1>, <task name 2>]
