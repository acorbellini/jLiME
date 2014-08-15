#!/bin/bash
JVM_OPTS="-Xmx4g"
CP="../../lib/*:./*"

#nohup java $JVM_OPTS -cp "$CP" edu.dkvs.Server $OPTS > server.out &
#nohup java -cp "./*" -Djava.net.preferIPv4Stack=true edu.dkvs.Server jgroups.xml twitter-data > server.out &
java $JVM_OPTS -cp "$CP" edu.jlime.collections.intintarray.loader.loader.Loader followees.prop
java $JVM_OPTS -cp "$CP" edu.jlime.collections.intintarray.loader.Loader followers.prop
