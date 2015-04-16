#!/bin/bash
JVM_OPTS="-Xmx4g"
CP="../../lib/*:./*"

NUMNODES=8

IN=$1
OUT=$2
GRAPHNAME=$3

echo "Loading edge"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader load $NUMNODES $GRAPHNAME " " $IN $OUT

echo "Validating edges"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.GraphlyLoader validate $NUMNODES $GRAPHNAME " " $IN $OUT