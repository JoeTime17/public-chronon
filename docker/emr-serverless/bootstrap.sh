#!/usr/bin/env bash
#
# Bootstrap the EMR Serverless custom image for a Zipline customer.
# Creates (idempotently) the ECR repo, builds the Docker image, and
# pushes the matching tag. Pair with the infra-aws-prod
# `emr_custom_image_version` Terraform variable (same tag value).
#
# Usage:
#   docker/emr-serverless/bootstrap.sh <customer-name> <version-tag>
#
# Env overrides:
#   AWS_REGION   AWS region (defaults to `aws configure get region`)
#
set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") <customer-name> <version-tag>

Creates the ECR repo chronon-emr-<customer-name>-custom-image (if missing),
builds the EMR Serverless custom image from this directory's Dockerfile, and
pushes it tagged <version-tag>. The same <version-tag> goes into
infra-aws-prod's emr_custom_image_version variable.

Arguments:
  customer-name   Customer slug (matches the customer_name TF variable).
                  Used in the ECR repo and the EMR app name.
  version-tag     Docker image tag — e.g. v3.3.2-spark-3.5.3.

Environment:
  AWS_REGION      AWS region. Falls back to \`aws configure get region\`.

Prerequisites:
  - aws CLI authed (\`aws sts get-caller-identity\` works)
  - docker daemon running

Examples:
  $(basename "$0") canary v3.3.2-spark-3.5.3
  AWS_REGION=us-west-2 $(basename "$0") canary v3.3.2-spark-3.5.3
EOF
}

if [ "$#" -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 2 ]; then
  echo "ERROR: expected exactly 2 arguments, got $#" >&2
  usage >&2
  exit 1
fi

CUSTOMER="$1"
TAG="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="chronon-emr-${CUSTOMER}-custom-image"

ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)
REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"

if [ -z "$ACCOUNT" ] || [ -z "$REGION" ]; then
  echo "ERROR: could not resolve AWS account or region." >&2
  echo "       Make sure 'aws sts get-caller-identity' works and AWS_REGION (or" >&2
  echo "       'aws configure get region') is set." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker daemon is not reachable. Start Docker Desktop / dockerd first." >&2
  exit 1
fi

REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_URI="${REGISTRY}/${REPO}:${TAG}"

echo "==> Account:   ${ACCOUNT}"
echo "==> Region:    ${REGION}"
echo "==> Repo:      ${REPO}"
echo "==> Tag:       ${TAG}"
echo "==> Image URI: ${IMAGE_URI}"
echo

if aws ecr describe-repositories --region "$REGION" --repository-names "$REPO" >/dev/null 2>&1; then
  echo "==> ECR repo ${REPO} already exists, skipping create."
else
  echo "==> Creating ECR repo ${REPO}..."
  aws ecr create-repository \
    --region "$REGION" \
    --repository-name "$REPO" \
    --image-tag-mutability MUTABLE >/dev/null
fi

echo "==> Building image..."
docker build --platform linux/amd64 -t "chronon-emr-serverless:${TAG}" "$SCRIPT_DIR"

echo "==> Logging in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

echo "==> Tagging + pushing..."
docker tag "chronon-emr-serverless:${TAG}" "$IMAGE_URI"
docker push "$IMAGE_URI"

echo
echo "==> Done. Set the following in your infra-aws-prod tfvars and run tofu apply:"
echo "    emr_custom_image_version = \"${TAG}\""
