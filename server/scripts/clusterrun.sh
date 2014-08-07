#!/bin/bash
cluster=$(cat $1)
user=$2
if [ -z $2 ]; then
	echo "Must put username."
	return 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

bash $DIR/stop.sh $1 $2
for i in $cluster; do
	nohup ssh $user@$i "cd /home/$user/jlime/scripts;. localrun.sh" &
done
