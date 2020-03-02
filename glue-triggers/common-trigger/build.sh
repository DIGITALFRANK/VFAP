# Build commands for Common glue job triggers
set -ue

module_name="glue-triggers/common-trigger"

# Valid values for update type is M for Major updates, m for minor updates and p for patch update. The update type is case sensitive.
update_type="m"

echo "===================[ Build: $module_name ]==================="

src_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname src -type d)
templates_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname templates -type d)
versioning_base_path="$CODEBUILD_SRC_DIR/versioning"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

### if no templates to update set template_path to NULL or uncomment following line
### template_path="NULL"
template_path="$templates_base_path/template.json"

### Alternatively we can also use following
### base_path=$(dirname "$0")

current_version=$(aws s3 ls $artifacts_base_path/ --recursive | grep zip | sort | tail -n 1 | awk '{print $4}' | awk -F '-' '{print $NF}' | cut -d '.' -f 1-3)

### Following command will get new version
new_version=$(python $versioning_base_path/semantic-version-v2.py $current_version $update_type)

### Following command will update the template with new version
python $versioning_base_path/update-template.py $new_version $template_path

# Packing lambda with dependencies
cd $src_base_path
zip -r common-trigger-$new_version.zip .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $templates_base_path/  $artifacts_base_path/templates/
aws s3 cp $src_base_path/common-trigger-$new_version.zip $artifacts_base_path/src/

echo "Build.sh completed"