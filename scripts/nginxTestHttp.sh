#!/bin/bash

docker pull nginx:stable

docker run \
  -p 8081:80 \
  -v ${PWD}/../spark/src/test/resources:/usr/share/nginx/html:ro \
  -d nginx:stable

