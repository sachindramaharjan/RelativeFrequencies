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
			map.plus(iter.next());

		map.calculateFrequency();
		output.write(key, map);
	}
}
