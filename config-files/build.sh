# Build commands for DynamoDB tables creation
set -ue

# Upload config files to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/config-files/ s3://vf-artifacts-bucket/vfap/config-files/

### temporary solution till we figure out the solution
### Copying the envrionment specific config_store.ini file

#aws s3 cp $CODEBUILD_SRC_DIR/config-files/dev/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/dev/config_store.ini
aws s3 cp $CODEBUILD_SRC_DIR/config-files/qa/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/qa/config_store.ini
# aws s3 cp $CODEBUILD_SRC_DIR/config-files/prod/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/prod/config_store.ini

echo "Build.sh completed"