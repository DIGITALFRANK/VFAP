# Build commands for parma store
set -ue

module_name="param-store"
echo "===========================[ Build: $module_name ]============================"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/$module_name/templates/  $artifacts_base_path/templates/

echo "Build.sh completed"