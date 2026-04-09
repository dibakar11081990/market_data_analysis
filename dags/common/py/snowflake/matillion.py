##################################################################################
"""
A Common Class for all the Matillion related Functions
"""
#############################################################################
# Imports
# requests for issuing api requests to Matillion
import requests
import time
import json

#############################################################################


class Matillion(object):
    """
    Central Class for managing all the API's related to Matillion.
    """

    def __init__(self, ip_address, matillion_userid, matillion_passwd):
        """
        Constructor
        :param ip_address:  Matillion Instance Ip Address
        :param matillion_userid: Matillion Instance User id
        :param matillion_passwd: Matillion Instance password.
        """

        # Set the Matillion ipaddress
        self.IP_ADDR = ip_address

        # Set the Matillion Userid
        self.USER_NAME = matillion_userid

        # Set the Matillion password
        self.PASSWORD = matillion_passwd

        # API to get all the groups in Matillion
        self.GROUP_API_URL = '{ip_address}/rest/v1/group'

        # API to get all the projects in Matillion
        self.PROJECT_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project'

        # API to get all the versions in a particular project
        self.VERSION_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/version'

        # API to get all the Jobs under a version in Matillion.
        self.JOB_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/' \
                           'version/name/{version_name}/job'

        # API to export the group as a JSON.
        self.EXPORT_GROUP_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/export'

        # API to export a job as a JSON.
        self.EXPORT_JOB_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/' \
                                  'version/name/{version_name}/job/name/{job_name}/export'

        # API to import a job given a JSON.
        self.IMPORT_JOB_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/' \
                                  'version/name/{version_name}/job/import'

        # API to run a job
        self.RUN_JOB_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/' \
                               'version/name/{version_name}/job/name/{job_name}/run?environmentName={env_name}'

        # API to check the Job Status
        self.TASK_STATUS_API_URL = '{ip_address}/rest/v1/group/name/{group_name}/project/name/{project_name}/' \
                                   'task/id/{task_id}'

    def get_groups(self):
        """
        Returns the List of groups present in Matillion as a list.
        :return: List containing group names
        """

        # Get the group names from Matillion
        group_api_res = requests.get(self.GROUP_API_URL.format(ip_address=self.IP_ADDR),
                                     auth=(self.USER_NAME, self.PASSWORD))

        # get the group list from the response
        group_list = group_api_res.json()

        if not isinstance(group_list, list):
            print("get_groups() Group API returned invalid data >>> " + str(group_list))
            raise Exception("Matillion get_groups() API Failure.")

        # return the obtained list.
        return group_list

    def get_projects(self, group_name):
        """
        Get all the projects under a particular group
        :param group_name: Group Name for which the projects needs to be obtained needs to be passed.
        :return: List containing the projects under a group.
        """

        # Issue the get request to get all the projects available under an Group.
        project_api_res = requests.get(self.PROJECT_API_URL.format(ip_address=self.IP_ADDR, group_name=group_name),
                                       auth=(self.USER_NAME, self.PASSWORD))

        # get the list of projects
        project_list = project_api_res.json()

        if not isinstance(project_list, list):
            print("get_projects() Project API Failed for group >>> {0} data >>> {1}".format(group_name,
                                                                                            str(project_list)))
            raise Exception("Matillion get_projects() API Failure.")

        # return the obtained project list
        return project_list

    def get_versions(self, group_name, project_name):
        """
        Get all the versions present under a particular Group / Project
        :param group_name: Group name
        :param project_name: Project Name
        :return: List containing all the versions under a Group/Project
        """

        # get the list of versions under GROUP/PROJECT
        version_api_res = requests.get(self.VERSION_API_URL.format(ip_address=self.IP_ADDR,
                                                                   group_name=group_name,
                                                                   project_name=project_name),
                                       auth=(self.USER_NAME, self.PASSWORD))

        # get the list of versions
        version_list = version_api_res.json()

        # check if the returned data is valid.
        if not isinstance(version_list, list):
            print("get_versions() Version List API Failed for group >>> {0} project >>> {1})".format(group_name,
                                                                                                     project_name,
                                                                                                     str(version_list)))
            raise Exception("Matillion get_versions() API Failure.")

        # return the obtained job list.
        return version_list

    def get_jobs(self, group_name, project_name, version_name):
        """
        Get all the jobs under a particular Group / Project / Version
        :param group_name: Group name
        :param project_name: Project name
        :param version_name: Version name
        :return: List containing all the jobs under a Group/Project/Version
        """

        # Get the list of Jobs present under GROUP/PROJECT/VERSION
        job_api_res = requests.get(self.JOB_API_URL.format(ip_address=self.IP_ADDR,
                                                           group_name=group_name,
                                                           project_name=project_name,
                                                           version_name=version_name),
                                   auth=(self.USER_NAME, self.PASSWORD))

        # get the list of Jobs
        job_list = job_api_res.json()

        # check if the returned data is valid.
        if not isinstance(job_list, list):
            print("get_jobs() Job List API Failed for "
                  "group >>> {0} project >>> {1} version >>> {2} data >>> {3})".format(group_name, project_name,
                                                                                       version_name, str(job_list)))
            raise Exception("Matillion get_jobs() API Failure.")

        # return the obtained job list.
        return job_list

    def export_job(self, group_name, project_name, version_name, job_name):
        """
        Export a particular job as a dictionary
        :param group_name: group name of the job
        :param project_name:  Project Name of the job
        :param version_name:  Version name of the job
        :param job_name: Job name which needs to be exported
        :return: Matillion Export JSON of the job.
        """

        # Issue the Export request for each of the job
        export_job_api_res = requests.get(self.EXPORT_JOB_API_URL.format(ip_address=self.IP_ADDR,
                                                                         group_name=group_name,
                                                                         project_name=project_name,
                                                                         version_name=version_name,
                                                                         job_name=job_name),
                                          auth=(self.USER_NAME, self.PASSWORD))

        # check if api returned valid data
        if not export_job_api_res:
            print("export_job() Job Export API Failed for "
                  "group >>> {0} project >>> {1} version >>> "
                  "{2} job >>> {3} data >>> {4})".format(group_name, project_name, version_name,
                                                         job_name, str(export_job_api_res.text)))

            raise Exception("Matillion export_job() API Failure")

        else:
            # Convert the Job export to a dictionary.
            job_export_dict = export_job_api_res.json()

            # return the job data as a dictionary
            return job_export_dict

    def export_group(self, group_name):
        """
        Export the whole group as a dictionary
        :param group_name: group name which needs to be exported
        :return: Matillion Export JSON of the group.
        """
        print(self.EXPORT_GROUP_API_URL.format(ip_address=self.IP_ADDR, group_name=group_name))
        # Issue the Export request for each of the job
        export_group_api_res = requests.get(self.EXPORT_GROUP_API_URL.format(ip_address=self.IP_ADDR,
                                                                             group_name=group_name),
                                            auth=(self.USER_NAME, self.PASSWORD))

        # check if api returned valid data
        if not export_group_api_res:
            print("export_group() group Export API Failed for group >>> {0} ".format(group_name))
            raise Exception("Matillion export_group() API Failure")
        else:
            # Convert the Job export to a dictionary.
            group_export_dict = export_group_api_res.json()

            # return the job data as a dictionary
            return group_export_dict

    def import_job(self, group_name, project_name, version_name, job_name, job_export_json):
        """
        Import a particular job.
        :param group_name: group name of the job
        :param project_name:  Project Name of the job
        :param version_name:  Version name of the job
        :param job_name: Job Name
        :param job_export_json: Job Export Json
        :return: Boolean True/False
        """

        headers = {
            'Content-Type': 'application/json'
        }

        # Issue the Post request to restore the job.
        import_res = requests.post(url=self.IMPORT_JOB_API_URL.format(ip_address=self.IP_ADDR,
                                                                      group_name=group_name,
                                                                      project_name=project_name,
                                                                      version_name=version_name),
                                   headers=headers,
                                   data=job_export_json,
                                   auth=(self.USER_NAME, self.PASSWORD))

        # check if the response is not fine
        if not import_res:
            print("import_job() Import Failed for job >>> {0} "
                  "response >>> {1} status_code >>> {2}".format(job_name, import_res.text, import_res.status_code))

            raise Exception("Matillion import_job() API Failure")
        else:
            return True

    def is_valid_group(self, group_name):
        """
        Function to check if a given group is a valid group
        :param group_name: group_name which needs to be validated
        :return: Boolean True/False
        """

        # get all the groups
        group_list = self.get_groups()

        # return the result.
        if group_name in group_list:
            return True
        else:
            return False

    def is_valid_project(self, group_name, project_name):
        """
        Function to check if a project is present under a group
        :param group_name: group name to which the project belongs to
        :param project_name: project name which needs to be validated
        :return: boolean True/False
        """

        # Get all the projects under a group
        project_list = self.get_projects(group_name=group_name)

        if project_name in project_list:
            return True
        else:
            return False

    def is_valid_job(self, group_name, project_name, version_name, job_name):
        """
        Function to check if a given job is a valid job under a certain project/group
        :param group_name: group name
        :param project_name: projectname
        :param version_name: version name
        :param job_name: job which needs to be validated
        :return: boolean True/False
        """

        # get all the jobs under the project
        job_list = self.get_jobs(group_name=group_name, project_name=project_name, version_name=version_name)

        if job_name in job_list:
            return True
        else:
            return False

    def run_job(self, group_name, project_name, version_name, job_name, env_name, body):
        """
        Use the Matillion API to run a job
        :param group_name: Matillion group name
        :param project_name: Matillion project name
        :param version_name: Matillion Version Name
        :param job_name: Job name in matillion which needs to be run
        :param env_name: Environment using which the job needs to run.
        :param body:  A dictionary containing the parameters required for the job to run.
        :return: job_id returned from Matillion.
        """

        # header
        headers = {
            'Content-Type': 'application/json'
        }

        # Issue the Post request to restore the job.
        response = requests.post(url=self.RUN_JOB_API_URL.format(ip_address=self.IP_ADDR,
                                                                 group_name=group_name,
                                                                 project_name=project_name,
                                                                 version_name=version_name,
                                                                 job_name=job_name,
                                                                 env_name=env_name),
                                 headers=headers,
                                 data=json.dumps(body),
                                 auth=(self.USER_NAME, self.PASSWORD))

        # check if the response is not fine
        if not response:
            print("run_job() Job Failed to run >>> {0} "
                  "response >>> {1} status_code >>> {2}".format(job_name, response.text, response.status_code))

            raise Exception("Matillion run_job() API Failure")
        else:

            # get the data returned by the API.
            """
            Sample data would be as follows.
            {
                "success": true,
                "msg": "Successfully queued job testing_1",
                "id": 36398
            }
            """

            # convert the response to json
            response_dict = response.json()

            # return the job id.
            return response_dict['id']

    def get_job_status(self, group_name, project_name, task_id):
        """
        Get the status for a particular job given its task_id
        :param group_name: Matillion group name
        :param project_name: Matillion Project Name
        :param task_id: task id for which the status needs to be obtained.
        :return: success / failure. Raise exception in case of failure.
        """

        # sleep for 15 seconds before starting
        time.sleep(15)

        # header for the api get request.
        headers = {
            'Content-Type': 'application/json'
        }

        # url from which we can get the job status.
        url = self.TASK_STATUS_API_URL.format(ip_address=self.IP_ADDR,
                                              group_name=group_name,
                                              project_name=project_name,
                                              task_id=task_id)

        while True:

            # Issue the Post request to restore the job.
            response = requests.get(url, headers=headers, auth=(self.USER_NAME, self.PASSWORD))

            # check if the response is not fine
            if not response:
                print("get_job_status() API Failure response >>> {0} status_code >>> {1}".format(
                    response.text, response.status_code))

                raise Exception("Matillion get_job_status() API Failure")
            else:

                # convert the response to json
                response_dict = response.json()

                # check if the job is running
                if response_dict['state'] == 'RUNNING':

                    print("get_job_status() Task with id {0} is currently in RUNNING STATE. "
                          "Sleeping for 15 sec.".format(task_id))

                    # sleep for 15 seconds
                    time.sleep(15)
                else:

                    # break if the task is not in running state.
                    break

        # check the final status of the job.
        if response_dict['state'] == 'SUCCESS':
            return True

        else:
            raise Exception("Matillion get_job_status() Task didn't succeed. Return Text >>> " + str(response.text))

    # Add all the Matillion related common functions here.
#########################################################################

