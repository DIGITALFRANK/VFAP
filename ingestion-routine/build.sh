# Build commands for ingestion routine

pip install -r requirements.txt --target $CODEBUILD_SRC_DIR/ingestion-routine/src/
mv $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/ingestion-routine/src/
find $CODEBUILD_SRC_DIR/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/param-resolver/src/
python3 param-resolver.py
cp $CODEBUILD_SRC_DIR/ingestion-routine/templates/template-resolved.json $CODEBUILD_SRC_DIR/
zip -r9 $CODEBUILD_SRC_DIR/ingestion-routine/src/ingestion-routine.zip $CODEBUILD_SRC_DIR/ingestion-routine/src/*
aws s3 cp $CODEBUILD_SRC_DIR/ingestion-routine/src/ingestion-routine.zip s3://vf-artifacts-bucket/vfap/ingestion-routine/src/