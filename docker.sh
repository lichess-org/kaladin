#!/bin/sh

target=$1

if [ "$target" = "cpu" ]
then
	docker build --build-arg TARGET="" -t kaladin-tensorflow-jupyter:2.4.1-jupyter . && \
	docker run -it --rm \
	--network=host \
	kaladin-tensorflow-jupyter:2.4.1-jupyter
elif [ "$target" = "gpu" ]
then
	docker build --build-arg TARGET="-gpu" -t kaladin-tensorflow-jupyter:2.4.1-gpu-jupyter . && \
	docker run -it --rm \
	--network=host \
	--gpus all \
	kaladin-tensorflow-jupyter:2.4.1-gpu-jupyter
else
	echo "target can only be cpu or gpu, not $target"
	exit 1
fi 