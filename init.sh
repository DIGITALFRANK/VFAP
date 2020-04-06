#Please select following modules you want to build
#	1 ) 	config-files
#	2 ) 	dynamoDB
#	3 ) 	glue-conn
#	4 ) 	glue-triggers/common-trigger
#	5 ) 	glue-triggers/merge-trigger
#	6 ) 	ingest-routine
#	7 ) 	param-store
#	8 ) 	refine-routine
#	9 ) 	secrets
#	10) 	transform-routine
#	11)		glue-jobs

#buildModules="glue-jobs"

buildModules="ingest-routine,refine-routine,glue-triggers/common-trigger"

for module in $(echo $buildModules | sed "s/,/ /g")
do
    find $CODEBUILD_SRC_DIR/$module/ -iname build.sh -type f -exec sh {} \;
done
