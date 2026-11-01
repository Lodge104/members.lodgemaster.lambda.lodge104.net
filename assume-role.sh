#!/bin/bash
# Assumes an IAM role for use by subsequent CodeBuild commands within the
# same buildspec phase. Exports temporary credentials as environment
# variables so that the AWS CLI and SAM CLI pick them up automatically.
#
# Usage: . ./assume-role.sh <role-arn> <role-session-name>

set -euo pipefail

ROLE_ARN="$1"
SESSION_NAME="$2"

CREDENTIALS=$(aws sts assume-role \
  --role-arn "${ROLE_ARN}" \
  --role-session-name "${SESSION_NAME}" \
  --query 'Credentials' \
  --output json)

export AWS_ACCESS_KEY_ID=$(echo "${CREDENTIALS}" | python3 -c "import sys, json; print(json.load(sys.stdin)['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo "${CREDENTIALS}" | python3 -c "import sys, json; print(json.load(sys.stdin)['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo "${CREDENTIALS}" | python3 -c "import sys, json; print(json.load(sys.stdin)['SessionToken'])")
