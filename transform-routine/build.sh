# Build commands for merge glue job triggers
set -ue
# Resolve Mapping parameter values for template.json
find $CODEBUILD_SRC_DIR/transform-routine/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/param-resolver/src/
python3 param-resolver.py

# Packing lambda with dependencies
cd $CODEBUILD_SRC_DIR/transform-routine/src/
zip -r transform-routine .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/transform-routine/templates/  s3://vf-artifacts-bucket/vfap/transform-routine/templates/
aws s3 sync $CODEBUILD_SRC_DIR/transform-routine/src/ s3://vf-artifacts-bucket/vfap/transform-routine/src/

echo "Build.sh completed"

