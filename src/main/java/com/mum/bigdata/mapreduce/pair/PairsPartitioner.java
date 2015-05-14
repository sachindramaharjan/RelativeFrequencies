package com.mum.bigdata.mapreduce.pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairsPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair pair, IntWritable value, int numReducer) {
		return pair.getFirst().hashCode() % numReducer;
	}
}
