#!/bin/bash

set -ex

TAG=${1}
USER='hellej'
DOCKER_IMAGE=${USER}/mqtt-sub-monitor:${TAG}
DOCKER_IMAGE_LATEST=${USER}/mqtt-sub-monitor:latest

docker build -t ${DOCKER_IMAGE} .

docker tag ${DOCKER_IMAGE} ${DOCKER_IMAGE_LATEST}
docker login
docker push ${DOCKER_IMAGE}
docker push ${DOCKER_IMAGE_LATEST}
