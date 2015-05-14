package com.mum.bigdata.mapreduce.pair;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mum.bigdata.mapreduce.pair.Pair;

public class PairsMapperWithCombiner extends
		Mapper<Object, Text, Pair, IntWritable> {
	private Pair pair = new Pair();
	private Pair dummypair = new Pair();
	private HashMap<String, Integer> map;

	@Override
	protected void setup(Mapper<Object, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);

		map = new HashMap<String, Integer>();
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] data = value.toString().split("\\s+");

		if (data.length > 0) {
			for (int i = 0; i < data.length; i++) {
				String term = data[i];

				if (term.length() == 0)
					continue;

				dummypair.set(term, "*");

				for (int j = i + 1; j < data.length; j++) {
					String neighbour = data[j];

					if (neighbour.length() == 0) {
						continue;
					} else if (neighbour.equals(term)) {
						break;
					} else {
						pair.set(term, neighbour);
						if (map.containsKey(pair.getKey())) {
							map.put(pair.getKey(), map.get(pair.getKey()) + 1);
						} else {
							map.put(pair.getKey(), 1);
						}

						if (map.containsKey(dummypair.getKey())) {
							map.put(dummypair.getKey(),
									map.get(dummypair.getKey()) + 1);
						} else {
							map.put(dummypair.getKey(), 1);
						}
					}
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Pair pair = new Pair();
		for (String p : map.keySet()) {
			pair.set(p);
			context.write(pair, new IntWritable(map.get(p)));
		}
	}

}
