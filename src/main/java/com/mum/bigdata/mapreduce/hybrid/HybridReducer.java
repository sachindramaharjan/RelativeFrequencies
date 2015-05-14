package com.mum.bigdata.mapreduce.hybrid;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mum.bigdata.mapreduce.stripes.Stripe;

public class HybridReducer extends
		Reducer<HybridPair, IntWritable, Text, Stripe> {

	private Stripe stripe;
	private String previouskey = null;

	@Override
	protected void setup(
			Reducer<HybridPair, IntWritable, Text, Stripe>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		stripe = new Stripe();
	}

	@Override
	protected void reduce(HybridPair pair, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		String key = pair.getFirst().toString();

		if (!key.equals(previouskey) && previouskey != null) {
			stripe.setFrequency();
			context.write(new Text(previouskey), stripe);
			stripe.clear();
		}

		int sum = 0;
		for (IntWritable i : values) {
			sum += i.get();
		}

		previouskey = key;
		stripe.put(new Text(pair.getSecond()), new IntWritable(sum));
	}

	/*
	 * executes at the end (emits last stripe)
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);

		if (stripe != null && previouskey != null) {
			stripe.setFrequency();
			context.write(new Text(previouskey), stripe);
		}
	}

}
