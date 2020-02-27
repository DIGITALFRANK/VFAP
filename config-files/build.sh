# Build commands for DynamoDB tables creation
set -ue

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/config-files/ s3://vf-artifacts-bucket/vfap/config-files/

echo "Build.sh completed"