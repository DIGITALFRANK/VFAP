# Build commands for merge glue job triggers
set -ue

module_name="glue-jobs"
echo "========================[ Build: $module_name ]========================="

src_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname src -type d)
templates_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname templates -type d)
versioning_base_path="$CODEBUILD_SRC_DIR/versioning"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

# Packing lambda with dependencies
mkdir $src_base_path/site-packages/
cd $src_base_path/site-packages/
zip -r site-packages.zip .
cp site-packages.zip $src_base_path/

cd $src_base_path/
zip -r $module_name.zip .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $templates_base_path/  $artifacts_base_path/templates/
aws s3 sync $src_base_path/ $artifacts_base_path/src/

echo "Build.sh completed"

