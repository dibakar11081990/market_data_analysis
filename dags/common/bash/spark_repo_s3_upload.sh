#!/usr/bin/env bash

# Variables
ROOT_DIR=${MARS_ROOT_DIR}
EMR_S3_BUCKET=com.tredence_analytics.marketing.mars.${ENVIRONMENT}.ue1.emr-artifacts
if [ "$ENVIRONMENT" == "dev" ]
then
    BRANCH=develop
else
    BRANCH=master
fi


# Resolve script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Assume project root is /usr/local/airflow (parent of dags)
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
TARGET_DIR="${PROJECT_ROOT}/other_repos"
echo "Script dir      : $SCRIPT_DIR"
echo "Project root    : $PROJECT_ROOT"
echo "Target (exists?): $([ -d "$TARGET_DIR" ] && echo YES || echo NO)"


#below wont work as the astro wont be having to do any sort of git command on the repo
# switch the MPM Spark repo directory and pull in the latest code.
# cd ${ROOT_DIR}/mars_mpm_spark
# git pull origin ${BRANCH}

# # Switch to the cfn repo directory
# cd ${ROOT_DIR}/mars_cloudformation_templates
# git pull origin master


# # Build the Spark code
# cd ${ROOT_DIR}/mars_mpm_spark/python
# make clean
# make build

# #### AWS Operations

# # Remove the previous spark artifacts
# aws s3 rm s3://${EMR_S3_BUCKET}/mars_spark/jobs.zip
# aws s3 rm s3://${EMR_S3_BUCKET}/mars_spark/driver.py

# # Upload the generated driver and jobs.zip to EMR-artifacts S3 Bucket.
# aws s3 cp ${ROOT_DIR}/mars_mpm_spark/python/dist/driver.py s3://${EMR_S3_BUCKET}/mars_spark/
# aws s3 cp ${ROOT_DIR}/mars_mpm_spark/python/dist/jobs.zip s3://${EMR_S3_BUCKET}/mars_spark/

# # Upload the EMR cfn template
# aws s3 cp ${ROOT_DIR}/mars_cloudformation_templates/emr_mpm/mpm_ingestion_emr_stack.yaml s3://${EMR_S3_BUCKET}/
