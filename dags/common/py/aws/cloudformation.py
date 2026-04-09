#############################################################################
"""
A Common Class for handling the AWS Cloudformation API's
"""
#############################################################################
# Imports
import boto3
import pandas as pd
import time
import json
from dags.activity_marketo_v2_driver.utility_functions import aws_conn_credentials
#############################################################################


def paginate(func, **kwargs):
    """
    Iterate through boto3's nextToken's
    :param func:  function which needs to be called
    :param kwargs: keyword arguments to the function
    :return: get all the page results from the function response.
    """

    # call the passed function and get the response
    response = func(**kwargs)

    # get the stack events
    stack_events = response.get('StackEvents', [])

    # check if the response has a nextToken
    while response.get('nextToken') is not None:
        # pass the token for pagination
        kwargs['nextToken'] = response['nextToken']

        # call the function again with the required modified arguments
        response = func(**kwargs)

        # get all the events again.
        stack_events.extend(response.get('StackEvents', []))

    # sort all the stack events by timestamp.
    sorted_stack_events = sorted(stack_events, key=lambda job: job['Timestamp'])

    # Sorted stack events
    return sorted_stack_events


class CloudformationException(Exception):
    """
    Exception handling class.
    """
    pass


class Cloudformation(object):
    """
    Central Class for Handling the BOTO3 Cloudformation API.
    """

    # Stack Valid Status list http://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_Stack.html
    VALID_STACK_STATUSES = ['CREATE_IN_PROGRESS', 'CREATE_FAILED', 'CREATE_COMPLETE', 'ROLLBACK_IN_PROGRESS',
                            'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE', 'DELETE_IN_PROGRESS', 'DELETE_FAILED',
                            'DELETE_COMPLETE', 'UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                            'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_ROLLBACK_FAILED',
                            'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE',
                            'REVIEW_IN_PROGRESS']

    # Our Default region in aws is US-EAST1
    DEFAULT_REGION = 'us-east-1'

    # CFN Admin Role ARN
    CFN_ADMIN_ROLE_ARN = 'arn:aws:iam::690264454041:role/AdskCfnAdministratorAccessExecutionRole'

    def __init__(self, region=None):
        """
        :param region: AWS region
        :type region: str
        """
        ACCESS_KEY, SECRET_KEY = aws_conn_credentials()
        # create a client to cloudformation which can be used to create / delete / update / describe stacks.
        self.cfn_client = boto3.client('cloudformation', region_name=region or Cloudformation.DEFAULT_REGION, aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)

        # create a cfn resource
        self.cfn_resource = boto3.resource('cloudformation', region_name=region or Cloudformation.DEFAULT_REGION, aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)

    def stack_exists(self, name):
        """
        Check if a CloudFormation  stack exists

        :param name: stack name
        :return: True/False
        """

        # Check if the provided stack exists in cloudformation.
        return name.lower() in [stack.stack_name.lower() for stack in self.cfn_resource.stacks.all()
                                if stack.stack_status in [state for state in Cloudformation.VALID_STACK_STATUSES
                                                          if state != 'DELETE_COMPLETE']]

    def describe_stack_events(self, name, pandas=False):
        """
        Describe CFN stack events

        :param name: stack name
        :param pandas: boolean indicating if the output should be returned in pandas Dataframe format.
        :return: stack events
        """

        # get all the stack events
        stack_events = paginate(self.cfn_client.describe_stack_events, StackName=name)

        if pandas:
            # convert it to a dataframe
            df = pd.DataFrame(stack_events)

            # filter only the required columns
            cols = [column for column in
                    ['ResourceType', 'PhysicalResourceId', 'ResourceStatus', 'Timestamp'] if column in df.columns]
            df = df[cols]

            # return the final dataframe
            return df
        else:
            return stack_events

    def describe_stack(self, name):
        """
        Describe CFN stack

        :param name: stack name
        :return: stack object as a JSON.
        """

        return self.cfn_client.describe_stacks(StackName=name)

    def _parse_template(self, template):
        """
        Parses the template file and checks if the template is valid.
        :param template: template file which needs to be read and validate.
        :return: validated template.
        """
        print(template)
        print(template[0:5])
        if template[0:5] != 'https':
            with open(template) as template_fileobj:
                template_data = template_fileobj.read()

            # validate if the template is fine.
            self.cfn_client.validate_template(TemplateBody=template_data)

            # return the template data
            return template_data

        else:

            # validate if the template is fine.
            self.cfn_client.validate_template(TemplateURL=template)
            # return empty data
            return None


    def tail_stack_events(self, name):
        """
        Tail the events which the stack is generating
        :param name: name of the stack for which events needs to be tailed.
        :return: if the Stack Failed or Succeeded.
        """

        previous_stack_events = 0
        stack_status = 0

        # Loop until there is a success or a failure.
        while True:

            # sleep for 60 seconds.
            time.sleep(3)

            # describe the stack to get the current status
            stack = self.describe_stack(name)

            # get all the events which the stack is generating
            stack_events = self.describe_stack_events(name)

            # current len_stack_events is > previous_event_list
            if len(stack_events) > previous_stack_events:

                # print the event list
                for event in stack_events[previous_stack_events or None:]:

                    print(json.dumps({'resource_type': event['ResourceType'],
                                      'logical_resource_id': event['LogicalResourceId'],
                                      'physical_resource_id': event['PhysicalResourceId'],
                                      'resource_status': event['ResourceStatus'],
                                      'timestamp': str(event['Timestamp'])},
                                     indent=3))

                previous_stack_events = len(stack_events)

            # Check the current status of the stack
            if stack['Stacks'][0]['StackStatus'].endswith('_FAILED') or \
                    stack['Stacks'][0]['StackStatus'] in ('ROLLBACK_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'):
                break
            elif stack['Stacks'][0]['StackStatus'].endswith('_COMPLETE'):
                stack_status = 1
                break

        # return the status
        return stack_status

    def create_stack(self, name, template, parameters):
        """
        This function creates the cloudformation templates

        :param name: stack name
        :param template: YAML encodeable object
        :param parameters: dictionary containing key value pairs as CFN parameters
        """

        # Stack timeout
        stack_timeout = 20

        try:

            # Template Validation
            print("create_stack() Validating the Cloudformation Template. Stack >>> " + str(name))
            template_data = self._parse_template(template)
            print("create_stack() Cloudformation Template Verification Succeeded. Stack >>> " + str(name))

            if isinstance(parameters, str):
                parameters = json.loads(parameters)

            if template_data:
                # parameters for stack creation
                params = {
                    'StackName': name,
                    'TemplateBody': template_data,
                    'Parameters': parameters,
                    'TimeoutInMinutes': stack_timeout,
                    'RoleARN': Cloudformation.CFN_ADMIN_ROLE_ARN,
                    'Tags': [
                        {
                            'Key': 'name',
                            'Value': name
                        }
                    ]
                }
            else:
                # parameters for stack creation
                params = {
                    'StackName': name,
                    'TemplateURL': template,
                    'Parameters': parameters,
                    'TimeoutInMinutes': stack_timeout,
                    'RoleARN': Cloudformation.CFN_ADMIN_ROLE_ARN,
                    'Tags': [
                        {
                            'Key': 'name',
                            'Value': name
                        }
                    ]
                }

            # issue the command the create the stack
            print("create_stack() Issuing the Create Stack Command. Stack >>> " + str(name))
            self.cfn_client.create_stack(**params)

            # tail the events generated by the stack
            print("create_stack() Printing Stack Events. Stack >>> " + str(name))
            if self.tail_stack_events(name=name):
                print("create_stack() Stack Creation Successful. Stack >>> "+str(name))
            else:
                print("create_stack() Stack Creation Failed. Stack >>> " + str(name))
                raise CloudformationException("Stack Creation Failed. Check the events for RootCause.")

            # create the waiter object
            # waiter = self.cfn_client.get_waiter('stack_create_complete')
            # wait for the stack to get created
            # waiter.wait(StackName=name)

        except Exception as ex:
            raise CloudformationException('create_stack() Exception StackName >>> %s ErrorMessage >>> %s' % (name, ex))

    def delete_stack(self, name):
        """
        This function would be used to delete a cloudformation stack
        :param name: stack name which needs to be deleted.
        :return:
        """
        try:

            print("delete_stack() Stack Deletion Initiated. Stack >>> "+str(name))

            # Call the function which deletes the stack
            self.cfn_client.\
                delete_stack(StackName=name,
                             RoleARN=Cloudformation.CFN_ADMIN_ROLE_ARN)

            # create the waiter object
            waiter = self.cfn_client.get_waiter('stack_delete_complete')

            # wait for the stack to get created
            waiter.wait(StackName=name)

            print("delete_stack() Stack Deletion Successful. Stack >>> " + str(name))

        except Exception as ex:
            raise CloudformationException('delete_stack() Exception StackName >>> %s ErrorMessage >>> %s' % (name, ex))

    def update_stack(self, name, template, parameters):
        """
        This function would be used to update an existing Cloudformation Stack
        :param name:  Name of the cloudformation template which needs to be updated
        :param template: Template file name
        :param parameters:  Template parameters.
        :return:
        """

        try:

            # Template Validation
            print("update_stack() Validating the Cloudformation Template. Stack >>> " + str(name))
            template_data = self._parse_template(template)
            print("update_stack() Cloudformation Template Verification Succeeded. Stack >>> " + str(name))

            # parameters for stack creation
            params = {
                'StackName': name,
                'TemplateBody': template_data,
                'Parameters': parameters,
                'RoleARN': Cloudformation.CFN_ADMIN_ROLE_ARN
            }

            # issue the command the create the stack
            print("update_stack() Update Stack Initiated. Stack >>> " + str(name))
            self.cfn_client.update_stack(**params)

            # create the waiter object
            waiter = self.cfn_client.get_waiter('stack_update_complete')
            # wait for the stack to get created
            waiter.wait(StackName=name)

            print("update_stack() Stack Update Successful. Stack >>> " + str(name))

        except Exception as ex:
            raise CloudformationException('update_stack() Exception StackName >>> %s ErrorMessage >>> %s' % (name, ex))