# Cassandra Dataset Manager

Cassandra Dataset Manager, referred to simply as `cdm`, is a tool to learn how to use Cassandra.

Work in progress.  Original cdm was written in Python but is being ported to Java for maintainability and the ability to easy distribute a single file.

## Building

You can build an executable via the following command:

`src/main/sh/build_runnable.sh`

This will create a JAR with a bash wrapper that you can drop in your $PATH and run like a normal Linux/Mac executable.

CDM doesn't currently work on windows as it's dependent on bash and cqlsh.  There are plans to lift this restriction.
