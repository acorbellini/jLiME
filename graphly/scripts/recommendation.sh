#!/bin/bash
JVM_OPTS="-Xmx512m -Djava.net.preferIPv4Stack=true"
CP="../../lib/*:./*"

java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.ModelComparison remote ~/jlime/extra/graphly acorbellini cluster konnect
