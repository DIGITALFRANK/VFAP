# Build commands for ingestion routine

# Resolve Mapping parameter values for template.json
find $CODEBUILD_SRC_DIR/ -iname template.json > $CODEBUILD_SRC_DIR/param-resolver/src/templates.txt
cd $CODEBUILD_SRC_DIR/param-resolver/src/
python3 param-resolver.py


# Packing lambda with dependencies
cd $CODEBUILD_SRC_DIR/
pip install -r requirements.txt --target $CODEBUILD_SRC_DIR/ingestion-routine/src/
mv $CODEBUILD_SRC_DIR/utils/* $CODEBUILD_SRC_DIR/ingestion-routine/src/
cd $CODEBUILD_SRC_DIR/ingestion-routine/src/
zip ingestion-routine.zip ./*
aws s3 cp $CODEBUILD_SRC_DIR/ingestion-routine/src/ingestion-routine.zip s3://vf-artifacts-bucket/vfap/ingestion-routine/src/

# Update the lambda function
aws lambda list-functions | grep ingestion-routine > /dev/null 2>&1
if [ 0 -eq $? ]; then
	echo "Lambda exists. Update the zip file"
	aws lambda update-function-code --function-name vf-dev-ingestion-routine  --s3-bucket vf-artifacts-bucket --s3-key vfap/ingestion-routine/src/ingestion-routine.zip
	aws lambda update-function-code --function-name vf-qa-ingestion-routine  --s3-bucket vf-artifacts-bucket --s3-key vfap/ingestion-routine/src/ingestion-routine.zip
else
	echo "Lambda does not exist. Don't update the code"
fi

# Upload templates to artifacts-bucket
aws s3 sync $CODEBUILD_SRC_DIR/ingestion-routine/templates/  s3://vf-artifacts-bucket/vfap/ingestion-routine/templates/