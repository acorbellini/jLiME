#!/bin/bash
cluster="$(cat $1)"
user=$2
if [ -z $2 ]; then
	echo "Indicate username"
	return 1
fi
for i in $cluster;
do
	ssh-copy-id $user@$i
done