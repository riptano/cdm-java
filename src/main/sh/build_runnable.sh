#!/bin/sh

rm -rf target/*jar-with-dependencies.jar
mvn -e package

mkdir bin

cat src/main/sh/execute.sh target/*jar-with-dependencies.jar > bin/cdm && chmod 755 bin/cdm
