#!/bin/bash
JVM_OPTS="-Xmx8g"
CP="dkvs.jar"

#nohup java $JVM_OPTS -cp "$CP" edu.dkvs.Server $OPTS > server.out &
#nohup java -cp "./*" -Djava.net.preferIPv4Stack=true edu.dkvs.Server jgroups.xml twitter-data > server.out &
nohup java $JVM_OPTS -cp "$CP" edu.dkvs.loader.DKVSLoader followees.prop > loader1.out &
nohup java $JVM_OPTS -cp "$CP" edu.dkvs.loader.DKVSLoader followers.prop > loader2.out &
