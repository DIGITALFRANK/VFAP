# Build commands for DynamoDB tables creation
set -ue

module_name="config-files"
echo "===========================[ Build: $module_name ]==========================="
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

# Upload config files to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/$module_name/ $artifacts_base_path/

# aws s3 cp $CODEBUILD_SRC_DIR/$module_name/dev/dl/config_store.ini $artifacts_base_path/dev/dl/config_store.ini
# aws s3 cp $CODEBUILD_SRC_DIR/$module_name/qa/dl/config_store.ini $artifacts_base_path/qa/dl/config_store.ini
# aws s3 cp $CODEBUILD_SRC_DIR/$module_name/prod/dl/config_store.ini $artifacts_base_path/prod/dl/config_store.ini

echo "Build.sh completed"