#!/bin/bash
JVM_OPTS="-Xmx4g -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
CP="../../lib/*:./*"

echo "Obtaining top"
java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.TopUsersExtractor $1 ~/twitter/top.users 0.5 10