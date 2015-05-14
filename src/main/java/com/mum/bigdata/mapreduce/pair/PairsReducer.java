package com.mum.bigdata.mapreduce.pair;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mum.bigdata.mapreduce.util.Utility;

public class PairsReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	IntWritable total = new IntWritable(0);
	DoubleWritable frequency = new DoubleWritable();
	private Text symbol = new Text("*");
	private Text initval = new Text("");

	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		if (key.getSecond().equals(symbol)) {
			if (key.getFirst().equals(initval)) {
				total.set(total.get() + getSum(values));
			} else {
				initval.set(key.getFirst());
				total.set(getSum(values));
			}
		} else {
			int cnt = getSum(values);
			double freq = Utility.formatNumber((double) cnt / total.get());
			frequency.set(freq);
			context.write(key, new DoubleWritable(freq));
		}
	}

	public int getSum(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		return sum;
	}
}
