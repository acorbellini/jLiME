#!/bin/bash
JVM_OPTS="-Xmx128m -Djava.net.preferIPv4Stack=true"
CP="../../lib/*:./*"

java $JVM_OPTS -cp "$CP" edu.jlime.graphly.util.ModelComparison ~/jlime/extra/graphly acorbellini cluster konnect
