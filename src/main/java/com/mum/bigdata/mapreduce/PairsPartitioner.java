package com.mum.bigdata.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.mum.bigdata.util.Pair;

public class PairsPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair pair, IntWritable value, int numReducer) {
		
		if(numReducer == 0)
			return 0;
		
		if(pair.getFirst().toString().substring(0,1).compareTo("5") < 0)
			return 0;
		else
			return 1;
	}

}
