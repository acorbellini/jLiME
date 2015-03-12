#!/bin/bash
JVM_OPTS="-Xmx4g"
CP="../../lib/*:./*"

echo "Loading edges"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader load 8 $1 " " out
echo "Loading IN edges"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader load 8 $2 " " in

echo "Validating OUT edges"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader validate 8 $1 " " out
echo "Validating IN edges"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader validate 8 $2 " " in