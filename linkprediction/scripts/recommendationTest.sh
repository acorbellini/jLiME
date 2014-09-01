#!/bin/bash
userList=$1
CP="../../lib/*:./*"
function run {
 while read user; do
	for i in {1..5}; do
		if [ ! -e "/home/acorbellini/results/$1/$user/$user-profile-net-$1-run$i.csv" ]; then
			java -Djava.net.preferIPv4Stack=true -cp "$CP" edu.jlime.linkprediction.twitter.RecommendationTest config.xml $1 $i $user /home/acorbellini/jlime
		else
		    echo "Recommendation $i for $user already exists" 
		fi
	done
 done < $userList
}

#run "locaware"
run "totalmemory"
run "availablememory"
run "roundrobin"
