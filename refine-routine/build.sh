# Build commands for Refine routine
set -ue
# Resolve Mapping parameter values for template.json
# Resolve Mapping parameter values for template.json
#find $CODEBUILD_SRC_DIR/dynamoDB/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
#cd $CODEBUILD_SRC_DIR/param-resolver/src/
#python3 param-resolver.py

# Copy all VFAP utils
mkdir $CODEBUILD_SRC_DIR/refine-routine/src/utils

cp $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/refine-routine/src/utils/

# Packing lambda with dependencies
cd $CODEBUILD_SRC_DIR/refine-routine/src/
zip -r refine-routine-V-1.1.0.zip .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/refine-routine/templates/  s3://vf-artifacts-bucket/vfap/refine-routine/templates/
aws s3 cp $CODEBUILD_SRC_DIR/refine-routine/src/refine-routine-V-1.1.0.zip s3://vf-artifacts-bucket/vfap/refine-routine/src/

echo "Build.sh completed"