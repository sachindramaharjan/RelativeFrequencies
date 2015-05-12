package com.mum.bigdata.mapreduce.stripes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, Stripe, Text, Stripe> {

	@Override
	public void reduce(Text key, Iterable<Stripe> values, Context output)
			throws IOException, InterruptedException {

		Iterator<Stripe> iter = values.iterator();
		Stripe map = new Stripe();

		while (iter.hasNext())
			map.sum(iter.next());

		map.setFrequency();
		output.write(key, map);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}
}
