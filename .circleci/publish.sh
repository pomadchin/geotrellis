#!/bin/bash

VERSION=`echo $SCALA_VERSION | cut -f1-2 -d "."`

if [ $VERSION = "2.12" ]; then
    echo "here";
else
    echo "here2";
fi
