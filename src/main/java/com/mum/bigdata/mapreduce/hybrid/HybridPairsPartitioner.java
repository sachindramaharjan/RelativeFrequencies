package com.mum.bigdata.mapreduce.hybrid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HybridPairsPartitioner extends Partitioner<HybridPair, IntWritable> {

	@Override
	public int getPartition(HybridPair pair, IntWritable value, int numReducer) {
		return pair.getFirst().hashCode() % numReducer;
	}

}
