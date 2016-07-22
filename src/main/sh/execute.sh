#!/bin/sh
MYSELF=`which "$0" 2>/dev/null`

[ $? -gt 0 -a -f "$0" ] && MYSELF="./$0"

java=java

if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
fi

if ! hash cqlsh 2>/dev/null; then
    echo "ERROR: cqlsh not found in path"
    exit 0
fi

exec "$java" -Xmx1G -Xms256M $java_args -jar $MYSELF "$@"
exit 1