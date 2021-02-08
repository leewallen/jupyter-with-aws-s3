#!/bin/bash

FOUND_JQ=`which jq`

if [ "$FOUND_JQ" == "" ]; then
   echo -e "Couldn't find jq in your path. Please 'brew install jq' if using macOS, or 'choco install jq' if using Windows."
   exit 1
fi

echo -e "\nThis script assumes that you have valid AWS credentials in your $HOME/.aws/credentials file."

if [ -f "$HOME/.aws/credentials" ]; then
  echo -e "\nReading credentials from credential file.\n"
else
  echo -e "\nCredentials file was not found at $HOME/.aws/credentials. Please create the credentials file before running this script.\n"
  exit 1
fi

creds_json=$(aws --profile default --region us-west-2 sts get-session-token)

docker run -d --name jupyter --rm -p 8888:8888 \
  -v `pwd`/notebooks/:/home/joyvan/work/ \
  -e AWS_ACCESS_KEY_ID=$(echo "$creds_json" | jq -r .Credentials.AccessKeyId) \
  -e AWS_SECRET_ACCESS_KEY=$(echo "$creds_json" | jq -r .Credentials.SecretAccessKey) \
  -e AWS_SESSION_TOKEN=$(echo "$creds_json" | jq -r .Credentials.SessionToken) \
  -e GRANT_SUDO=yes \
  leewallen/jupyter-docker:f7b90d697c9c jupyter lab --ip=0.0.0.0 --allow-root --LabApp.token ''

echo -e "Use 'localhost:8888' in your browser to access JupyterLab\n"

