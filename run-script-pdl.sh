# NOTE that input given to output path of splitter and combiner are different
# $1=name $2=dataset $3=lda_simplex $4=minStepSizeLimit $5=initialStepSize $6=1st dimension (N) $7=2nd dimension MN) $8=rank $9=d

#time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-splitter /user/abeutel/netflix /user/abhimank/sample_output/PSGD/run0
#time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-splitter /user/abeutel/netflix /user/abeutel/PSGD/run0
#time hadoop jar PSGDCombiner.jar PSGDCombiner -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-combiner /user/abeutel/PSGD/run0 /user/abeutel/PSGD/run0/data

dataset_path=$2
N=$6    # 100
M=$7   # 1000
P0=1
key="psgd-"$1
lambda=10
mean=1
sparse=0
kl=0
nnmf=1
rank=$8
d=$9
initialStep="$5" 
debug=1
lda_simplex=$3
min=$4
#no_wait=$6
#shuffleList=${11}
#last_iter=$(echo "scale=10; $d*$d-1" | bc -l)
last_iter=$(echo "scale=10; $d-1" | bc -l)

echo -e "Key: $key\nLambda: $lambda\nMean: $mean\nSparse: $sparse\nNNMF: $nnmf\nKL: $kl\nRank: $rank\nd: $d\ninitialStep: $initialStep\nminStep: $min\ndebug: $debug\nlda_simplex: $lda_simplex\nLast iteration: $last_iter" > ~/log-$key.txt


#hadoop fs -rmr /user/abeutel/$key/*
hadoop fs -rmr /user/abhimank/$key/*
hadoop fs -rmr /user/abhimank/.Trash
#rm ~/loss-$key.txt
#rm ~/time-$key.txt

params=" -D psgd.regularizerLambda=$lambda -D psgd.initMean=$mean -D psgd.nnmf=$nnmf -D psgd.sparse=$sparse -D psgd.KL=$kl -D psgd.N=$N -D psgd.M=$M -D psgd.rank=$rank -D psgd.debug=$debug -D psgd.lda_simplex=$lda_simplex  $d $key "

echo -e $params >>~/log-$key.txt

#output_dir="/user/abeutel/$key"
output_dir="/user/abhimank/$key"


echo "Iteration 0 Splitter"
step=$initialStep
echo "Step ${step}"
time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.stepSize=$step $params $dataset_path ${output_dir}/run0

last=0
for i in {1..2}
do

	echo "Iteration ${last} Combiner"
	echo "Step ${step}"

	time hadoop jar PSGDCombiner.jar PSGDCombiner -D psgd.stepSize=$step $params ${output_dir}/run$last ${output_dir}/run${last}/data

	echo "Iteration ${last} Frobenius"
	echo "Step ${step}"
	echo "Loss Iteration ${last}"
	time hadoop jar FrobeniusPSGD.jar FrobeniusPSGD $params $dataset_path ${output_dir}-loss/run$last ${output_dir}/run${last}
	
	step=$(echo "scale=10; $initialStep / (($i + 1) * 0.5)" | bc -l)
	step=$(echo $min $step | awk '{if ($1 < $2) print $2; else print $1}')

	echo "Iteration ${i} Splitter"
	echo "Step ${step}"

    time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.stepSize=$step $params $dataset_path ${output_dir}/run$i ${output_dir}/run$last


	last=$i
done
							  
