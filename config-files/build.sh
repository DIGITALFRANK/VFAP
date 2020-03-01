# Build commands for DynamoDB tables creation
set -ue

module_name="config-files"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

# Upload config files to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/$module_name/ $artifacts_base_path/

#aws s3 cp $CODEBUILD_SRC_DIR/$module_name/dev/config_store.ini $artifacts_base_path/dev/config_store.ini
aws s3 cp $CODEBUILD_SRC_DIR/$module_name/qa/config_store.ini $artifacts_base_path/qa/config_store.ini
# aws s3 cp $CODEBUILD_SRC_DIR/$module_name/prod/config_store.ini $artifacts_base_path/prod/config_store.ini

echo "Build.sh completed"