# testing build.sh

set -ue

module_name="testing"
echo "=============================[ Build: $module_name ]=============================="

src_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname src -type d)
templates_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname templates -type d)
versioning_base_path="$CODEBUILD_SRC_DIR/versioning"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

### if no templates to update set template_path to NULL or uncomment following line
### template_path="NULL"
template_path="$templates_base_path/merge-gluejob-lambda.json"

### Alternatively we can also use following
### base_path=$(dirname "$0")

current_version=$(aws s3 ls $artifacts_base_path/ --recursive | grep zip | sort | tail -n 1 | awk '{print $4}' | awk -F '-' '{print $NF}' | cut -d '.' -f 1-3)

### Following command will get new version
new_version=$(python $versioning_base_path/semantic-version-v2.py $current_version m)

### Following command will update the template with new version
python $versioning_base_path/update-template.py $new_version $template_path

### Upload artifacts
aws s3 cp $templates_base_path/merge-gluejob-lambda.json $artifacts_base_path/templates/

echo "Build.sh completed"