package com.mum.bigdata.mapreduce.stripes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable, Text, Text, Stripe> {

	private Stripe stripe = new Stripe();
	private Text textKey = new Text();

	@Override
	public void map(LongWritable key, Text value, Context output)
			throws IOException, InterruptedException {
		String text = value.toString();
		String[] numbers = text.split("\\s+");

		for (int i = 0; i < numbers.length; i++) {
			String num = numbers[i];

			stripe.clear();

			for (int j = i + 1; j < numbers.length; j++) {
				if (num.equals(numbers[j]))
					break;
				if (stripe.containsKey(numbers[j])) {
					stripe.increment(numbers[j], 1);
				} else {
					stripe.put(new Text(numbers[j]), new IntWritable(1));
				}
			}

			textKey.set(num);
			output.write(textKey, stripe);
		}
	}
}
