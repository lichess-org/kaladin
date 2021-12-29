#!/bin/sh

target=${1:-dev} # dev | prod

if [ "$target" = "prod" ]
then
	python3 queue_manager.py
elif [ "$target" = "dev" ]
then
	bash
else
	echo "target can only be dev or prod, not $target"
	exit 1
fi 