#!/bin/bash

function getMem {
	echo $(free -m | tr -s ' ' | head -2 | tail -1 | cut -d' ' -f2)
}

CP="../lib/*:../extra/webmonitor/webapp-1.0.jar"

OPTIONS="-Djava.net.preferIPv4Stack=true"
currMem=$(getMem)
mem=$(( $currMem - 1000))
OPTIONS=$OPTIONS" -Xmx"$mem"m"

nohup java -cp "$CP" $OPTIONS edu.jlime.server.Server > log.out 2>&1 &
