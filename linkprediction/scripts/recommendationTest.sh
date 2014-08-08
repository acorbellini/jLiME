#!/bin/bash
userList=$1
CP="../../lib/*:./*"
function run {
 while read user; do
	for i in {1..10}; do
		java -Djava.net.preferIPv4Stack=true -cp "$CP" edu.jlime.linkprediction.twitter.RecommendationTest config.xml $1 $i $user /home/acorbellini/jlime
	done
 done < $userList
}

#run "locaware"
#run "totalmemory"
#run "availablememory"
run "roundrobin"
