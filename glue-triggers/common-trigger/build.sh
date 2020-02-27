# Build commands for Common glue job triggers
set -ue
# Resolve Mapping parameter values for template.json
#find $CODEBUILD_SRC_DIR/dynamoDB/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
#cd $CODEBUILD_SRC_DIR/param-resolver/src/
#python3 param-resolver.py

# Packing lambda with dependencies
cd $CODEBUILD_SRC_DIR/glue-triggers/common-trigger/src/
zip -r common-gluejob-trigger-V.1.1.0.zip .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/glue-triggers/common-trigger/templates/  s3://vf-artifacts-bucket/vfap/glue-triggers/common-trigger/templates/
aws s3 cp $CODEBUILD_SRC_DIR/glue-triggers/common-trigger/src/common-gluejob-trigger-V.1.1.0.zip s3://vf-artifacts-bucket/vfap/glue-triggers/common-trigger/src/

echo "Build.sh completed"