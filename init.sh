#!/bin/sh

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

# buildModules=("config-file" "dynamoDB" "glue-conn" "glue-triggers/common-trigger" "glue-triggers/merge-trigger" "ingest-routine" "param-store" "refine-routine" "secrets" "transform-routine")


buildModules="config-files"

for module in $(echo $buildModules | sed "s/,/ /g")
do
    find $CODEBUILD_SRC_DIR/$module/ -iname build.sh -type f -exec sh {} \;
done

#for module in "${buildModules[@]}"
#do
#	find $CODEBUILD_SRC_DIR/$module/ -name build.sh -type f -exec sh {} \;
#done
