package com.mum.bigdata.mapreduce.hybrid;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HybridMapper extends Mapper<Object, Text, HybridPair, IntWritable> {

	private String term, neighbour;
	private HybridPair pair;
	private HashMap<String, Integer> hashmap;

	public HybridMapper() {
		pair = new HybridPair();
		hashmap = new HashMap<>();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] data = value.toString().split("\\s+");

		for (int i = 0; i < data.length; i++) {
			term = data[i];

			if (term.length() == 0)
				continue;

			for (int j = i + 1; j < data.length; j++) {
				neighbour = data[j];
				if (term.equals(neighbour))
					break;

				pair.set(term, neighbour);

				if (hashmap.containsKey(pair.getKey())) {
					hashmap.put(pair.getKey(), hashmap.get(pair.getKey()) + 1);
				} else {
					hashmap.put(pair.getKey(), 1);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		HybridPair pair = new HybridPair();
		for (String key : hashmap.keySet()) {
			pair.set(key);
			context.write(pair, new IntWritable(hashmap.get(key)));
		}
	}
}
