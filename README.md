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


## Acknowledgments

The Kaladin repository was re-created when transitioning to open source to ensure that user data was not made public.  Git history was expunged during that transition.  A record of the commits prior to the transition can be found [here](doc/lost_commits.txt):  
Special thanks to:
* [kraktus](https://github.com/kraktus) for your work on the queue manager, Docker config, error handling, lila integration, and integration testing.
* [michael1241](https://github.com/michael1241) for your domain expertise, design discussions, initial queue manager and mongo and deployment support.
* [ornicar](https://github.com/ornicar) for your support, your mongo wizardry, and your lila integration work.
* the others around the globe who helped by validating the model output, generating ideas, and providing valuable feedback.