#!/bin/bash

function getMem {
	echo $(free -m | tr -s ' ' | head -2 | tail -1 | cut -d' ' -f2)
}

CP="../../lib/*:./*"

OPTIONS="-Djava.net.preferIPv4Stack=true"
currMem=$(getMem)
mem=$(( $currMem - 500))
OPTIONS=$OPTIONS" -Xmx"$mem"m"

if [ $(hostname) == "GridCluster1" ]; then
	coord="true"
else
 	coord="false"
fi	
nohup java -cp "$CP" $OPTIONS edu.jlime.graphly.server.GraphlyServer twitter ~/GraphlyDB/ 1 $coord 8  > log.out 2>&1 &
