#!/bin/bash

# Increment a version string using Semantic Versioning.

# Parse command line options.

while getopts ":Mmp" Option
do
  case $Option in
    M ) major=true;;
    m ) minor=true;;
    p ) patch=true;;
  esac
done

shift $(($OPTIND - 1))

aws s3 cp s3://vf-artifacts-bucket/vfap/semantic-versioning/src/current.txt $CODEBUILD_SRC_DIR/semantic-versioning/src/current.txt
version=$(cat $CODEBUILD_SRC_DIR/semantic-versioning/src/current.txt | cut -d '-' -f 2)

echo $version

# Build array from version string.
a=()
for i in ${version//./ }
do
        a+=("$i")
done

echo "${a[@]}"

# If version string is missing or has the wrong number of members, show usage message.
#if [ ${#a[@]} -ne 2 ]
#then
#  echo "usage: $(basename $0) [-Mmp]"
#  exit 1
#fi

# Increment version numbers as requested.
if [ ! -z $major ]
then
  ((a[0]++))
  a[1]=0
  a[2]=0
fi

if [ ! -z $minor ]
then
  ((a[1]++))
  a[2]=0
fi

if [ ! -z $patch ]
then
  ((a[2]++))
fi

echo "Update version number in current.txt file"
new_version="V-${a[0]}.${a[1]}.${a[2]}"
echo "V-${a[0]}.${a[1]}.${a[2]}" > $CODEBUILD_SRC_DIR/semantic-versioning/src/current.txt 
aws s3 cp $CODEBUILD_SRC_DIR/semantic-versioning/src/current.txt s3://vf-artifacts-bucket/vfap/semantic-versioning/src/

echo "New updated version: $new_version"

echo "update main.json template with new version"
jq --arg a $new_version '.Parameters.Version.Default = $a' main.json > temp.json && mv temp.json main.json