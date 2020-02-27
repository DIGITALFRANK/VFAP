# Build commands for parma store
set -ue
# Resolve Mapping parameter values for template.json
find $CODEBUILD_SRC_DIR/param-store/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/param-resolver/src/
python3 param-resolver.py

# Install External libraries

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/param-store/templates/  s3://vf-artifacts-bucket/vfap/param-store/templates/

echo "Build.sh completed"