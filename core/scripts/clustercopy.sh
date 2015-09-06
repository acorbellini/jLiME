#!/bin/bash
# El cluster separado por ",": grid1,grid2 ... o 192.168.240.1,192.168.240.2 ...
cluster=$(echo $1 | tr ',' ' ')
# El usuario para logearse
user=$2
# Carpeta origen
orig=$3
# Carpeta destino (va a ser la misma en cada cluster)
dest=$4

PID_List=""
for i in $cluster
do
	echo Executing rsync on $i
	# --delete borra archivos que no esten en el origen y si en el destino
	# --checksum calcula diferencias por checksum para no copiar repetidos
	# --progress muestra el progreso
	# -z comprime los datos
	# -t mantiene los tiempos de modificación locales
	# -r recursivo
	rsync --delete --checksum -z --progress -tr $orig $user@$i:$dest &
	PID_List="$PID_List $!"
done

echo "Waiting for copy to complete..."
wait $PID_LIST

echo "Copy Finished."
