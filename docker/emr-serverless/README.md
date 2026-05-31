# EMR Serverless Custom Image

Custom EMR Serverless 7.12.0 Spark image that swaps the bundled `delta-spark`
jar for a Zipline-patched build. Everything else (Spark 3.5.3, Hadoop,
`delta-storage`, etc.) is left untouched.

See the [AWS custom images guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-custom-images.html).

## What the Dockerfile does

1. `FROM ${EMR_BASE_IMAGE}` — pinned by digest (default
   `public.ecr.aws/emr-serverless/spark/emr-7.12.0@sha256:...`) so rebuilds
   are reproducible. The public ECR `:latest` tag is mutable and AWS rotates
   it on new EMR releases; see the Dockerfile header for how to look up the
   digest when bumping.
2. Drops the local `delta-spark` jar into `/usr/share/aws/delta/lib/`.
3. Repoints the `delta-spark.jar` symlink at the new jar — EMR's
   `spark-defaults.conf` puts the **symlink** (not the versioned filename) on
   `spark.driver.extraClassPath` / `spark.executor.extraClassPath`, so leaving
   it dangling drops Delta off the classpath.
4. Ends as `USER hadoop:hadoop` (required by EMR Serverless).

## How to use it

The patched `delta-spark` jar is committed alongside this Dockerfile
(built from the [zipline-ai/delta](https://github.com/zipline-ai/delta)
fork — branch `v3.3.2-spark-3.5.3`). Customers don't need to build the
jar themselves — just build the image and push it to their own ECR.

The customer flow assumes `infra-aws-prod` is wired with the
`emr_custom_image_version` Terraform variable. Setting that variable
provisions a **sibling** EMR Serverless application
`zipline-emr-<customer_name>-custom-image` with the image attached, and
attaches an ECR repo policy granting EMR Serverless pull access scoped to
that app. The canonical `zipline-emr-<customer_name>` application is left
untouched, so paved-path workflows keep running on the default image until
you explicitly opt them in.

### Quick start

[`bootstrap.sh`](./bootstrap.sh) wraps steps 1-2 (ECR create, build, push)
into a single command. Use the same `<version-tag>` you'll put in tfvars.

```bash
./bootstrap.sh <customer-name> v3.3.2-spark-3.5.3
```

Then continue with step 3 below.

### Step-by-step

1. **Create the ECR repo (one-time).**
   ```bash
   aws ecr create-repository \
     --region <region> \
     --repository-name chronon-emr-<customer_name>-custom-image \
     --image-tag-mutability MUTABLE
   ```
   The repo name must follow this convention — Terraform attaches the pull
   policy to that exact name.

2. **Build and push the image.**
   ```bash
   cd <chronon-checkout>/docker/emr-serverless
   docker build --platform linux/amd64 -t chronon-emr-serverless:<tag> .

   ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
   REGION=${AWS_REGION:-$(aws configure get region)}
   if [ -z "$ACCOUNT" ] || [ -z "$REGION" ]; then
     echo "Set AWS_REGION (or aws configure set region) and ensure aws sts works before push." >&2
     exit 1
   fi
   aws ecr get-login-password --region "$REGION" \
     | docker login --username AWS --password-stdin \
         "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

   docker tag chronon-emr-serverless:<tag> \
     "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/chronon-emr-<customer_name>-custom-image:<tag>"
   docker push \
     "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/chronon-emr-<customer_name>-custom-image:<tag>"
   ```
   `--platform linux/amd64` is required when building on Apple Silicon
   (workers default to `x86_64`). `<tag>` must match the value you'll put in
   `emr_custom_image_version`. Convention: use the source branch name,
   e.g. `v3.3.2-spark-3.5.3`.

3. **Apply infra.** In `infra-aws-prod`, set
   `emr_custom_image_version = "<tag>"` in your tfvars and run
   `terraform apply`. The sibling app and repo policy are created;
   `imageConfiguration` is attached to the URI you just pushed.

4. **Opt workflows into the custom-image app via `teams.py`.** For each team
   whose workflows should run on the patched image, set
   `SPARK_CLUSTER_NAME = "zipline-emr-<customer_name>-custom-image"`. Other
   teams continue to use `zipline-emr-<customer_name>` and the default image.

5. **Run jobs.** The next backfill / job submission on an opted-in team
   starts the sibling app, pulls the image, and resolves the digest.

### Rebuilding the patched delta-spark jar

The bundled `delta-spark_2.12-3.3.2.jar` is the artifact of
`build/sbt -DsparkVersion=3.5 'project spark' package` against
[zipline-ai/delta@v3.3.2-spark-3.5.3](https://github.com/zipline-ai/delta/tree/v3.3.2-spark-3.5.3).
When that branch gets new patches, rebuild and commit the new jar in its
place.

### Updating to a new version

Push the new tag, bump `emr_custom_image_version` in your tfvars, then
`terraform apply`. EMR Serverless allows `imageConfiguration` updates only
when the application is `CREATED` or `STOPPED`; if the sibling app is
running, stop it first:

```bash
aws emr-serverless stop-application \
  --application-id <zipline-emr-{customer_name}-custom-image-id>
```

## Build args

- `EMR_BASE_IMAGE` (default
  `public.ecr.aws/emr-serverless/spark/emr-7.12.0@sha256:...`) — pinned by
  digest. Override to test a newer EMR release or alternate base image. See
  the Dockerfile header for how to fetch the current digest.
- `DELTA_SPARK_JAR` (default `delta-spark_2.12-3.3.2.jar`) — expected jar
  filename in the build context.
