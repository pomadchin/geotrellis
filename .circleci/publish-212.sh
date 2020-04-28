#!/bin/bash

./sbt -Dsbt.supershell=false "++$SCALA_VERSION" publishLocal
