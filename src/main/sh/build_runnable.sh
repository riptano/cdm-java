#!/bin/sh

mvn -e package

cat src/main/sh/execute.sh target/*jar-with-dependencies.jar > bin/cdm && chmod 755 bin/cdm
