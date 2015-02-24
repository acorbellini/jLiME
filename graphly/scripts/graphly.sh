#!/bin/bash

order=$1

function stop(){
	cluster=$1
	user=$2
	if [ -z $user ]; then
		echo "Must indicate username as second parameter."
		return 1;
	fi
	PID_List=""
	
	function killServer {
		LIST=$(ssh $user@$1 "ps -Af | grep edu.jlime.graphly.server.GraphlyServer | grep java | grep -v bash | tr -s ' ' | cut -d' ' -f2")
		for pid in $LIST;do
			ssh $user@$1 "kill -KILL $pid"
		done	
	}
	
	for i in $(cat cluster); do
		killServer $i & 
		PID_List="$PID_List $!"
	done
	
	echo "Waiting for kill to complete..."
	wait $PID_LIST
	
	echo "Kill Completed"
}

function start(){
	cluster=$1
	user=$2
	if [ -z $user ]; then
		echo "Must put username."
		return 1
	fi
	
	DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
	
	stop $cluster $user
	
	for i in $(cat cluster); do
		nohup ssh $user@$i "cd /home/$user/jlime/extra/graphly;. run.sh" &
	done
}

if [ $order == "stop" ]; then
	stop $2 $3
elif [ $order == "start" ]; then
	start $2 $3
fi


