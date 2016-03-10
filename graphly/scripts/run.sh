#!/bin/bash


sudo sysctl -w net.core.rmem_max=26214400
sudo sysctl -w net.core.wmem_max=26214400 

function getMem {
	echo $(free -m | tr -s ' ' | head -2 | tail -1 | cut -d' ' -f2)
}

CP="../../lib/*:./*"
#-XX:+UseNUMA -XX:+UseParallelGC -XX:+TieredCompilation -XX:+UseCompressedOops 
#  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false
OPTIONS="-Djava.net.preferIPv4Stack=true -XX:+UseCompressedOops -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"

currMem=$(getMem)

mem=$(( $currMem - 1024 ))

HOST=$(hostname)

#if [ "$HOST" == "GridCluster1" -o "$HOST" == "GridCluster3" -o "$HOST" == "GridCluster5" -o "$HOST" == "GridCluster11" ]; then
#	mem=2048
#fi


#if [ $HOST == "GridCluster10" ]; then
#	mem=4096
#fi

if [ $HOST == "GridCluster10" ]; then
	coord="true"
else
 	coord="false"
fi


OPTIONS=$OPTIONS" -Xmx"$mem"m"

nohup java -cp "$CP" $OPTIONS edu.jlime.graphly.server.GraphlyServer ~/GraphlyDB/ 1 $coord 8  > log.out 2>&1 &
