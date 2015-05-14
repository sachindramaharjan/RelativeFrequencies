package com.mum.bigdata.mapreduce.pair;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mum.bigdata.mapreduce.pair.Pair;

public class PairsMapper extends Mapper<Object, Text, Pair, IntWritable> {
	private Pair pair;
	private IntWritable one;

	public PairsMapper() {
		pair = new Pair();
		one = new IntWritable(1);
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

				for (int j = i + 1; j < data.length; j++) {
					String neighbour = data[j];

					if (neighbour.length() == 0) {
						continue;
					} else if (neighbour.equals(term)) {
						break;
					} else {
						pair.set(term, neighbour);
						context.write(pair, one);
						pair.set(term, "*");
						context.write(pair, one);
					}
				}
			}
		}
	}
}
