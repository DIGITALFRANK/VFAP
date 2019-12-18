# Build commands for ingestion routine

mv $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/ingestion-routine/src/
ls $CODEBUILD_SRC_DIR/ingestion-routine/src/
find $CODEBUILD_SRC_DIR/ -iname template-packaged.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
sam build -t $CODEBUILD_SRC_DIR/ingestion-routine/templates/template.yml
sam package --template-file $CODEBUILD_SRC_DIR/ingestion-routine/templates/template.yml --s3-bucket vf-artifacts-bucket --s3-prefix vfap/artifacts/ --output-template-file $CODEBUILD_SRC_DIR/ingestion-routine/templates/template-packaged.json --use-json
ls $CODEBUILD_SRC_DIR/ingestion-routine/templates/
python3 $CODEBUILD_SRC_DIR/param-resolver/src/param-resolver.py