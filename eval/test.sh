template=$1
node_count_start=$2
node_count_inc=$3
topology_count=$4
node_weight=$5

let node_count_max=node_count_start+node_count_inc*topology_count

echo Generating the topology files...
for i in $(seq $node_count_start $node_count_inc $node_count_max)
do
  	bash runtask.sh TopologyGenerator $template $i $node_weight topology${i}.yaml
done

echo Creating mappings...
for i in $(seq 1 2 3)
do 
	for j in $(seq $node_count_start $node_count_inc $node_count_max)
	do
		let k=j-node_count_inc
		bash runtask.sh CreateDataMapping yaml ${i} topology${j}.yaml map${i}${j}.csv rdfmap${k}.csv rdfmap${j}.csv
	done
done

echo Evaluate mappings...
for i in $(seq 1 2 3)
do 
	echo version ${i}:
	for j in $(seq $node_count_start $node_count_inc $node_count_max)
	do
		echo -n $topology${j}.yaml,
		let k=j-node_count_inc
		bash runtask.sh EvaluateMapping yaml topology${j}.yaml map${i}${j}.csv
		echo -n ,
		if [ -e map${i}${k}.csv ]
		then
			bash runtask.sh CalculateMovement map${i}${k}.csv map${i}${j}.csv
		else
			echo -n ,,
		fi
		echo
	done
done
