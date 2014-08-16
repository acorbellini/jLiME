cluster=$(cat $1)
user=$2
if [ -z $2 ]; then
	echo "Must indicate username as second parameter."
	return 1;
fi
PID_List=""

function killServer {
	LIST=$(ssh $user@$1 "ps -Af | grep edu.jlime.server.JobServer | grep java | grep -v bash | tr -s ' ' | cut -d' ' -f2")
	for pid in $LIST;do
		ssh $user@$1 "kill -KILL $pid"
	done	
}

for i in $cluster; do
	killServer $i & 
	PID_List="$PID_List $!"
done

echo "Waiting for kill to complete..."
wait $PID_LIST

echo "Kill Completed"