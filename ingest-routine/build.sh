# Build commands for ingestion routine
set -ue
# Resolve Mapping parameter values for template.json
find $CODEBUILD_SRC_DIR/ingest-routine/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/param-resolver/src/
python3 param-resolver.py


# Install External libraries
mkdir $CODEBUILD_SRC_DIR/ingest-routine/src/external_lib
pip install -r requirements.txt --target $CODEBUILD_SRC_DIR/ingest-routine/src/external_lib/

# Copy all VFAP utils
mkdir $CODEBUILD_SRC_DIR/ingest-routine/src/utils
cp $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/ingest-routine/src/utils/

# Packing lambda with dependencies
cd $CODEBUILD_SRC_DIR/ingest-routine/src/
zip -r ingest-routine-V.1.0.0.zip .

# Upload templates to artifacts-bucket
echo "Syncing the artifacts"
aws s3 sync $CODEBUILD_SRC_DIR/ingest-routine/templates/  s3://vf-artifacts-bucket/vfap/ingest-routine/templates/
aws s3 cp $CODEBUILD_SRC_DIR/ingest-routine/src/ingest-routine-V.1.0.0.zip s3://vf-artifacts-bucket/vfap/ingest-routine/src/

echo "Build.sh completed"