package com.mum.bigdata.mapreduce.stripes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StripePartitioner extends Partitioner<Text, Stripe> {

	@Override
	public int getPartition(Text key, Stripe value, int numReducer) {
		return key.hashCode() % numReducer;
	}

}
