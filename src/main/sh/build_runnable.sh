#!/bin/sh

mvn -e package

mkdir bin

cat src/main/sh/execute.sh target/*jar-with-dependencies.jar > bin/cdm && chmod 755 bin/cdm
