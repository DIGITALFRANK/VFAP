# Build commands for ingestion routine

pip install -r requirements.txt --target $CODEBUILD_SRC_DIR/ingestion-routine/src/
mv $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/ingestion-routine/src/
find $CODEBUILD_SRC_DIR/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/template-parser/src/
cat $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
python3 param-resolver.py
zip -r9 $CODEBUILD_SRC_DIR/ingestion-routine/src/ingestion-routine.zip $CODEBUILD_SRC_DIR/ingestion-routine/src/*
aws s3 sync $CODEBUILD_SRC_DIR/ s3://vf-artifacts-bucket/vfap/