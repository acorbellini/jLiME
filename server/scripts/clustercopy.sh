#!/bin/bash
cluster="$(cat $1)"
user=$2
if [ -z $2 ]; then
	echo "Must indicate username"
	return 1
fi
PID_List=""
for i in $cluster
do
	rsync --delete -tr ../ $user@$i:/home/$user/jlime &
	PID_List="$PID_List $!"
done

echo "Waiting for copy to complete..."
wait $PID_LIST

echo "Copy Finished."
