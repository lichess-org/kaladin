# Kaladin
This machine learning tool is aimed at automating cheat detection on Lichess using insights (https://lichess.org/insights/{USER}).

It is built using CNNs on Keras/TensorFlow.

## Setup
You will need:
 - Linux OS (tested on Ubuntu 20.04 LTS)
 - Docker
 - MongoDB

## Docker container setup for Tensorflow with CPU or gpu

### Pre-requisites
Install Docker using your favorite package manager, or for example you can [follow this guide](https://www.tensorflow.org/install/docker).

### Create custom image and container

Run `$./docker.sh gpu|cpu` with the needed target, it will create/update the image and start the container.

For production, you will need to run in the container `cd src && python3 queue_manager.py`


## Configuration

For the list of options and default values used by Kaladin, see `src/.env.base`. You can override these either by setting environmental variables or create a `src/.env` file.