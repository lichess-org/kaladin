#!/bin/bash

target=$1
shift # Subsequent calls to $@ will not contain $1 https://stackoverflow.com/a/9057699/11955835
echo "start.sh target"
echo "$@"
docker_sh_abs_path=$(dirname "$(readlink -f "$BASH_SOURCE")")
echo "$docker_sh_abs_path"

if [ "$target" = "cpu" ]
then
	docker build --build-arg TARGET="" -t kaladin-tensorflow-jupyter:2.4.1-jupyter "$docker_sh_abs_path" && \
	docker run -it --rm \
	--network=host \
	kaladin-tensorflow-jupyter:2.4.1-jupyter "$@"
elif [ "$target" = "gpu" ]
then
	docker build --build-arg TARGET="-gpu" -t kaladin-tensorflow-jupyter:2.4.1-gpu-jupyter "$docker_sh_abs_path" && \
	docker run -it --rm \
	--network=host \
	--gpus all \
	kaladin-tensorflow-jupyter:2.4.1-gpu-jupyter "$@"
else
	echo "target can only be cpu or gpu, not $target"
	exit 1
fi 