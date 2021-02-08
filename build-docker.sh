#!/bin/bash

image_name="leewallen/jupyter-docker"
tag=`date '+%Y%m%d-%H%M%S'`
echo -e "Tag: ${tag}"

CURR_DIR=`pwd`
cd jupyter-docker
docker build -t $image_name:$tag  -t $image_name:latest .
cd $CURR_DIR

echo -e "Tagged with $image_name:$tag and $image_name:latest"