# NOTE that input given to output path of splitter and combiner are different
# $1=name $2=dataset $3=lda_simplex $4=minStepSizeLimit $5=initialStepSize $6=1st dimension (M) $7=2nd dimension (N) $8=rank

#time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-splitter /user/abeutel/netflix /user/abhimank/sample_output/PSGD/run0
#time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-splitter /user/abeutel/netflix /user/abeutel/PSGD/run0
#time hadoop jar PSGDCombiner.jar PSGDCombiner -D psgd.M=17692 -D psgd.N=480189 -D psgd.rank=5 10 abhi-test-combiner /user/abeutel/PSGD/run0 /user/abeutel/PSGD/run0/data

dataset_path=$2
N=$7    # 100
M0=$6   # 1000
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


hadoop fs -rmr /user/abeutel/$key/*
#hadoop fs -rmr /user/abhimank/$key/*
#hadoop fs -rmr /user/abhimank/.Trash
#rm ~/loss-$key.txt
#rm ~/time-$key.txt

params=" -D psgd.regularizerLambda=$lambda -D psgd.initMean=$mean -D psgd.nnmf=$nnmf -D psgd.sparse=$sparse -D psgd.KL=$kl -D psgd.N=$N -D psgd.M0=$M0 -D psgd.rank=$rank -D psgd.debug=$debug -D psgd.lda_simplex=$lda_simplex  $d $key "

echo -e $params >>~/log-$key.txt

output_dir="/user/abeutel/$key"
#output_dir="/user/abhimank/$key"


echo "Iteration 0"
step=$initialStep
time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.stepSize=$step $params $dataset_path ${output_dir}/run0

last=0
for i in {1..15}
do

	time hadoop jar PSGDCombiner.jar PSGDCombiner -D psgd.stepSize=$step $params ${output_dir}/run$last ${output_dir}/run${last}/data

#	echo "Loss Iteration ${last}"
#	time hadoop jar Frobenius.jar Frobenius $params $dataset_path ${output_dir}-loss/run$last ${output_dir}/run${last}
	
	step=$(echo "scale=10; $initialStep / (($i + 1) * 0.5)" | bc -l)
	step=$(echo $min $step | awk '{if ($1 < $2) print $2; else print $1}')

	echo "Iteration ${i}"
	echo "Step ${step}"

    time hadoop jar PSGDRandomSplitter.jar PSGDRandomSplitter -D psgd.stepSize=$step $params $dataset_path ${output_dir}/run$i/data ${output_dir}/run$last


	last=$i
done
							  
