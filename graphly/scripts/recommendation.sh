#!/bin/bash
JVM_OPTS="-Xmx4g -Djava.net.preferIPv4Stack=true"
CP="../../lib/*:./*"

java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.RecommendationTest ~/graphly-results rec-config.xml
