#!/bin/bash
JVM_OPTS="-Xmx4g -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
CP="../../lib/*:./*"

echo "Query Test"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.QueryTest $1
