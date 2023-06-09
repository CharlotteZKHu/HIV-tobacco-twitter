#!/usr/bin/env bash
#$ -cwd
#$ -N carmen_tweets
#$ -j y -o $JOB_NAME-$JOB_ID.out
#$ -M caguirr4@jhu.edu
#$ -m e
#$ -l ram_free=5G,mem_free=5G
#$ -t 1-100


HOME="/export/c11/caguirr/tweet-collection"

ALL_FILES=(${HOME}/preprocessed_data/*.gz)
STEP=$((${#ALL_FILES[@]} / 100 + 1))
START_INDEX=$(((SGE_TASK_ID - 1) * STEP))
INPUT_FILES=("${ALL_FILES[@]:${START_INDEX}:$STEP}")

conda activate tweets

for i in "${INPUT_FILES[@]}"
do
	NAME="$(basename $i)"
	printf "running file %s\n" "$NAME"
	python -m carmen.cli "$i" "${HOME}/carmen_out_data/$NAME"
done	

