#!/usr/bin/env python3

'''
    Script Name : auto_dms_tasks.py
    Author : Debraj Ganguly
    Purpose :This script is intended to clone already created DMS Tasks and then re-create them using newer endpoints.
            The script is database independent and need to be run on the Ec2 instance which has the roles to access
            the DMS instance in question.
    Dependencies : dms_tasks_config.py which has the configuration values to be used in this script
'''


import dms_tasks_config as config
import sys
import json
import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(filename=config.dms_op_log_filepath, filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Process Started..')
print('Process Started..')

dms_client = boto3.client('dms', region_name=config.aws_region)

# num_arg = len(sys.argv) - 1

# Initializing the variables
old_src_end_point_arn = ''
cnt = 0
dict_src_arns = {}
dict_tgt_arns = {}
tsk_exists = 'N'


# Function to count the creation of tasks
def counter(read='N'):
    global cnt
    if read == 'Y':
        return cnt
    else:
        cnt = cnt + 1

    return cnt


# Function to check the validity of the Endpoint
def check_endpoint_arn(ReplicationInstanceArn, EndpointArn):
    try:
        response = dms_client.test_connection(
            ReplicationInstanceArn = ReplicationInstanceArn,
            EndpointArn = EndpointArn
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info('Connection testing for EndpointArn : {0} Passed'.format(EndpointArn))
            return True
        else:
            logging.info('Connection testing for EndpointArn : {0} Failed..'.format(EndpointArn))
            return False
    except dms_client.exceptions.InvalidResourceStateFault:
        logging.info('Connection testing for EndpointArn : {0} Passed'.format(EndpointArn))
        return True
    except ClientError as e:
        logging.exception(e)
        exit(1)


# Function to get all the endpoints for a particular endpoint type
def get_target_endpoints(endpoint_type='N'):

    if endpoint_type == 'N':
        endpoint_type = config.endpoint_type_val
    else:
        endpoint_type = endpoint_type

    kwargs = {
        "Filters": [
            {
                "Name": config.endpoint_type,
                "Values": [
                    endpoint_type
                ]
            }
        ],
        "MaxRecords": config.max_records,
        "Marker": ""
    }
    while True:
        response = dms_client.describe_endpoints(**kwargs)
        yield from response['Endpoints']
        try:
            kwargs['nextToken'] = response['nextToken']
        except KeyError:
            break


# Function to get the replication tasks for the old arn
def get_replication_tasks():
    kwargs = {
        "Filters": [
            {
                "Name": config.replication_task_filter,
                "Values": [
                    config.replication_instance_arn
                ]
            }
        ],
        "MaxRecords": config.max_records,
        "Marker": ""
    }
    while True:
        response = dms_client.describe_replication_tasks(**kwargs)
        yield from response['ReplicationTasks']
        try:
            kwargs['nextToken'] = response['nextToken']
        except KeyError:
            break


# Function to create new DMS tasks
def create_new_dms_tasks(ReplicationTaskIdentifier, SourceEndpointArn, TargetEndpointArn, ReplicationInstanceArn,
                         MigrationType, TableMappings, ReplicationTaskSettings):
    logging.info('Creating DMS task {0}'.format(ReplicationTaskIdentifier))
    try:
        response = dms_client.create_replication_task(
            ReplicationTaskIdentifier = ReplicationTaskIdentifier,
            SourceEndpointArn = SourceEndpointArn,
            TargetEndpointArn = TargetEndpointArn,
            ReplicationInstanceArn = ReplicationInstanceArn,
            MigrationType = MigrationType,
            TableMappings = TableMappings,
            ReplicationTaskSettings = ReplicationTaskSettings
        )
        status = response['ReplicationTask']['Status']
        ReplicationTaskCreationDate = response['ReplicationTask']['ReplicationTaskCreationDate'].replace(tzinfo=None)
        logging.info('Task created at {0}'.format(ReplicationTaskCreationDate))
        logging.info('The status for DMS task {0} is {1}'.format(ReplicationTaskIdentifier, status))
        counter()
        return True
    except dms_client.exceptions.ResourceAlreadyExistsFault:
        logging.info('The provided ReplicationTaskIdentifier {0} already exists hence skipping creation of this task and proceeding next..'.format(ReplicationTaskIdentifier))
        tsk_exists = 'Y'
        return True
    except Exception as e:
        logging.exception(e)
        exit(1)


# Function to edit the prefix for new task identifier
def edit_task_name_prefix(task_name):
    # Block to check the name prefix and edit it accordingly
    if task_name.startswith('non-prod'):
        edited_task_name = config.replicationtaskid_prefix + task_name.replace('non-prod-', '')
    elif task_name.startswith('prod-'):
        edited_task_name = config.replicationtaskid_prefix + task_name.replace('prod-', '')
    else:
        edited_task_name = config.replicationtaskid_prefix + task_name

    return edited_task_name


# Function to generate the mapping of endpoint identifiers and its corresponding ARNS for both Source and Target
def gen_endpoint_mapping_dict():
    logging.info('Generating the mappings for Source and Target End Point arns')

    try:
        for src_arns in get_target_endpoints('Source'):
            source_identifier = src_arns['EndpointIdentifier']
            SourceEndpointArn = src_arns['EndpointArn']

            dict_src_arns[SourceEndpointArn] = source_identifier

        logging.info('Generation of mappings for Source End Point arns is successfull..')

    except ClientError as e:
        logging.exception('Error in generating the source mappings arns : ' + e)
        exit(1)

    try:
        for tgt_arns in get_target_endpoints('Target'):
            target_endpoint_id = tgt_arns['EndpointIdentifier']
            TargetEndpointArn = tgt_arns['EndpointArn']

            dict_tgt_arns[TargetEndpointArn] = target_endpoint_id

        logging.info('Generation of mappings for Target End Point arns is successfull..' + '\n')

    except ClientError as e:
        logging.exception('Error in generating the Target mappings arns : ' + e)
        exit(1)


# Function to validate the Source and Target end points provided in config file
def validate_src_tgt_endpoints():
    logging.info('Starting validating of Source and Target mappings from config file')
    valid = 'Y'

    # Block to validate the Source mappings
    try:
        src_mapping_lst = []     # Create an empty list

        if config.use_arn_db_transforms == 'Y':
            for src_arns in get_target_endpoints('Source'):
                src_mapping_lst.append(src_arns['EndpointArn'])

        else:
            for src_arns in get_target_endpoints('Source'):
                src_mapping_lst.append(src_arns['EndpointIdentifier'])

        for kys, vals in config.src_endpoint_transforms.items():
            try:
                ret = src_mapping_lst.index(kys)
            except ValueError:
                valid = 'N'
                logging.info('Old Source Endpoint ARN/Identifier -> {0} is not valid..Kindly rectify and run again'.format(kys))

            try:
                ret = src_mapping_lst.index(vals)
            except ValueError:
                valid = 'N'
                logging.info('New Source Endpoint ARN/Identifier -> {0} is not valid..Kindly rectify and run again'.format(vals))

    except ClientError as e:
        logging.exception('Error in validating the Source mapping arns/identifiers : ' + e)
        exit(1)

    # Block to validate the Target mappings
    try:
        tgt_mapping_lst = []      # Create an empty list

        if config.use_arn_db_transforms == 'Y':
            for tgt_arns in get_target_endpoints('Target'):
                tgt_mapping_lst.append(tgt_arns['EndpointArn'])

        else:
            for tgt_arns in get_target_endpoints('Target'):
                tgt_mapping_lst.append(tgt_arns['EndpointIdentifier'])


        for kys, vals in config.tgt_endpoint_transforms.items():
            try:
                ret = tgt_mapping_lst.index(kys)
            except ValueError:
                valid = 'N'
                logging.info('Old Target Endpoint ARN/Identifier -> {0} is not valid..Kindly rectify and run again'.format(kys))

            try:
                ret = tgt_mapping_lst.index(vals)
            except ValueError:
                valid = 'N'
                logging.info('New Target Endpoint ARN/Identifier -> {0} is not valid..Kindly rectify and run again'.format(vals))

    except ClientError as e:
        logging.exception('Error in validating the Target mappings arns/identifiers : ' + e)
        exit(1)

    if valid == 'N':
        logging.info('Invalid Endpoints present in config file..Kindly validate and re-run again..')
        print('Invalid Endpoints present in config file..Kindly validate and re-run again..')
        exit(1)
    else:
        logging.info('Source and Target Mappings have been fully validated..')
        return src_mapping_lst, tgt_mapping_lst


# Function to edit the replication task settings to enable Cloudwatch Logs
def edit_task_settings(ReplicationTaskSettings):
    try:
        tsk_sttngs_dict = json.loads(ReplicationTaskSettings)    # converting the string to a dictionary object
        tsk_sttngs_dict['Logging']['EnableLogging'] = config.enable_logging
        tsk_sttngs_dict['Logging']['CloudWatchLogGroup'] = None
        tsk_sttngs_dict['Logging']['CloudWatchLogStream'] = None
        tsk_stngs_to_str = json.dumps(tsk_sttngs_dict)           # converting the dictionary back to string

        return tsk_stngs_to_str

    except ClientError as e:
        logging.exception('Error in editing ReplicationTaskSettings : ' + e)
        exit(1)


def main():
    new_target_endpoint_arn = ''
    new_source_endpoint_arn = ''
    reg_cnt = 0

    src_mapping_lst, tgt_mapping_lst = validate_src_tgt_endpoints()

    if config.use_arn_db_transforms == 'N':
        # Function calls to create the dictionary mapping for source and target arns
        gen_endpoint_mapping_dict()

    try:
        for event in get_replication_tasks():
            ReplicationTaskIdentifier = event['ReplicationTaskIdentifier']

            # Block to filter only on specific tasks
            if config.use_specific_tasks == 'Y':
                try:
                    task_nm = config.task_names.index(ReplicationTaskIdentifier)
                except ValueError:
                    continue

            rep_task_edited_name = edit_task_name_prefix(ReplicationTaskIdentifier)

            SourceEndpointArn = event['SourceEndpointArn']
            TargetEndpointArn = event['TargetEndpointArn']
            ReplicationInstanceArn = event['ReplicationInstanceArn']
            MigrationType = event['MigrationType']
            TableMappings = event['TableMappings']
            ReplicationTaskSettings = event['ReplicationTaskSettings']
            ReplicationTaskSettings = edit_task_settings(ReplicationTaskSettings)

            if config.use_arn_db_transforms == 'Y':
                try:
                    ret = src_mapping_lst.index(SourceEndpointArn)
                    new_source_endpoint_arn = config.src_endpoint_transforms[SourceEndpointArn]

                except ValueError:
                    continue
                except KeyError:
                    continue

                try:
                    ret = tgt_mapping_lst.index(TargetEndpointArn)
                    new_target_endpoint_arn = config.tgt_endpoint_transforms[TargetEndpointArn]

                except ValueError:
                    continue
                except KeyError:
                    continue

            elif config.use_arn_db_transforms == 'N':
                try:
                    new_src_endpnt_id = config.src_endpoint_transforms[dict_src_arns[SourceEndpointArn]]

                except KeyError:
                    continue

                for keys, values in dict_src_arns.items():
                    if values == new_src_endpnt_id:
                        new_source_endpoint_arn = keys

                try:
                    ret_id = dict_tgt_arns.get(TargetEndpointArn, 'None')

                    if ret_id != 'None':
                        new_tgt_endpnt_id = config.tgt_endpoint_transforms[ret_id]
                    else:
                        continue

                except KeyError:
                    continue

                for keys, values in dict_tgt_arns.items():
                    if values == new_tgt_endpnt_id:
                        new_target_endpoint_arn = keys

            if check_endpoint_arn(config.replication_instance_arn, new_source_endpoint_arn) \
                    and check_endpoint_arn(config.replication_instance_arn, new_target_endpoint_arn):

                if config.change_replication_instance == 'Y':
                    logging.info('Change in Replication instance..Switching to new replication instance for creation of new task..')
                    ReplicationInstanceArn = config.new_replication_inst_arn

                # Printing the values in the log file
                logging.info('''Parameters for the program are :
                        CurrentReplicationTaskIdentifier : {0}
                        NewReplicationTaskIdentifier     : {1}
                        CurrentReplicationInstanceArn    : {2}
                        CurrentSourceEndpointArn         : {3}
                        NewSourceEndpointArn             : {4}
                        CurrentTargetEndpointArn         : {5}
                        NewTargetEndpointArn             : {6}
                        MigrationType                    : {7}'''.format(ReplicationTaskIdentifier, rep_task_edited_name, ReplicationInstanceArn,
                                            SourceEndpointArn, new_source_endpoint_arn, TargetEndpointArn, new_target_endpoint_arn, MigrationType))

                if TargetEndpointArn == new_target_endpoint_arn \
                        and SourceEndpointArn != new_source_endpoint_arn:
                    SourceEndpointArn = new_source_endpoint_arn

                    logging.info('Change in Source End Point ARN. Hence proceeding to create tasks only for whose source arn changed..')

                    task_status = create_new_dms_tasks(rep_task_edited_name, SourceEndpointArn, TargetEndpointArn,
                                                       ReplicationInstanceArn, MigrationType, TableMappings, ReplicationTaskSettings)

                elif SourceEndpointArn == new_source_endpoint_arn \
                        and TargetEndpointArn != new_target_endpoint_arn:
                    TargetEndpointArn = new_target_endpoint_arn

                    logging.info('Change in Target End Point ARN. Hence proceeding to create tasks only for whose target arn changed..')

                    task_status = create_new_dms_tasks(rep_task_edited_name, SourceEndpointArn, TargetEndpointArn,
                                                       ReplicationInstanceArn, MigrationType, TableMappings, ReplicationTaskSettings)
                else:
                    SourceEndpointArn = new_source_endpoint_arn
                    TargetEndpointArn = new_target_endpoint_arn
                    logging.info('Change in both Source and Target End Point ARN. Hence proceeding to create tasks only for whose target arn changed..')

                    task_status = create_new_dms_tasks(rep_task_edited_name, SourceEndpointArn, TargetEndpointArn,
                                                       ReplicationInstanceArn, MigrationType, TableMappings, ReplicationTaskSettings)

                if task_status:
                    logging.info('Continuing with next task..' + '\n')
                    # break
                else:
                    logging.info('Terminating the loop..')
                    break
            else:
                logging.exception('Provided Target End point is not valid/active...Please rectify and run again')
                print('Provided Target End point is not valid/active...Please rectify and run again')
                exit(1)
        else:
            if counter('Y') > 0:
                logging.info('{0} tasks have been successfully created..'.format(counter('Y')))
                print('{0} tasks have been successfully created..'.format(counter('Y')))
            elif reg_cnt > config.src_endpoint_counts and tsk_exists != 'N':
                logging.info('Provided ARNs are not part of the replication tasks..Hence no tasks are created')
                print('Provided ARNs are not part of the replication tasks..Hence no tasks are created')
            else:
                logging.info('No tasks have been created..')
                print('No tasks have been created..')

    except ClientError as e:
        logging.exception(e)
        exit(1)


if __name__ == '__main__':
    main()
