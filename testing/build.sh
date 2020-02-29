# Build commands for DynamoDB tables creation
set -ue

module_name="testing"

src_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname src -type d)
templates_base_path=$(find $CODEBUILD_SRC_DIR/$module_name/  -iname templates -type d)
versioning_base_path="$CODEBUILD_SRC_DIR/versioning"
artifacts_base_path="s3://vf-artifacts-bucket/vfap/$module_name"

### if no templates to update set template_path to NULL or uncomment following line
### template_path="NULL"
template_path="$templates_base_path/merge-gluejob-lambda.json"

### Alternatively we can also use following
### base_path=$(dirname "$0")

current_version=$(aws s3 ls $artifacts_base_path/ --recursive | sort | tail -n 1 | awk '{print $4}' | grep zip | awk -F '-' '{print $NF}' | awk {'print substr($1,1,5)'})

### Following command will get new version
new_version=$(python $versioning_base_path/semantic-version-v2.py current_version m)

### Following command will update the template with new version
python $versioning_base_path/update-template.py new_version template_path

aws s3 cp $templates_base_path/merge-gluejob-lambda.json $artifacts_base_path/templates/

# Upload config files to artifacts-bucket
#echo "Syncing the artifacts"
#aws s3 sync $CODEBUILD_SRC_DIR/config-files/ s3://vf-artifacts-bucket/vfap/config-files/

### temporary solution till we figure out the solution
### Copying the envrionment specific config_store.ini file

#aws s3 cp $CODEBUILD_SRC_DIR/config-files/dev/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/dev/config_store.ini
#aws s3 cp $CODEBUILD_SRC_DIR/config-files/qa/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/qa/config_store.ini
# aws s3 cp $CODEBUILD_SRC_DIR/config-files/prod/config_store.ini s3://vf-artifacts-bucket/vfap/config-files/prod/config_store.ini

echo "Build.sh completed"