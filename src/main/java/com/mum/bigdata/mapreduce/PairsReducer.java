package com.mum.bigdata.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mum.bigdata.util.Pair;

public class PairsReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	IntWritable total = new IntWritable(0);
	DoubleWritable frequency = new DoubleWritable();
	private Text firstValue = new Text("");
	private Text asterisk = new Text("*");

	DecimalFormat df = new DecimalFormat("#.000");

	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		if (key.getSecond().equals(asterisk)) {
			if (key.getFirst().equals(firstValue)) {
				total.set(total.get() + getSum(values));
			} else {
				firstValue.set(key.getFirst());
				total.set(getSum(values));
			}
		} else {
			int cnt = getSum(values);
			double freq = Double.parseDouble(df.format((double) cnt
					/ total.get()));
			frequency.set(freq);
			context.write(key, frequency);
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
