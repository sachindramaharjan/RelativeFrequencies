package com.mum.bigdata.mapreduce.stripes;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.mum.bigdata.mapreduce.util.Utility;

public class Stripe extends MapWritable {

	public Stripe() {
		super();
	}

	public static Stripe create(DataInput in) throws IOException {
		Stripe m = new Stripe();
		m.readFields(in);

		return m;
	}

	public void sum(Stripe m) {
		for (Writable key : m.keySet()) {
			if (this.containsKey(key)) {
				this.put(key,
						new IntWritable(((IntWritable) this.get(key)).get()
								+ ((IntWritable) m.get(key)).get()));
			} else {
				this.put(key, m.get(key));
			}
		}
	}

	public void setFrequency() {

		int sum = 0;
		for (Writable key : keySet()) {
			sum += ((IntWritable) get(key)).get();
		}
		for (Writable key : keySet()) {

			this.put(
					key,
					new DoubleWritable(
							Utility.formatNumber((double) ((IntWritable) this
									.get(key)).get() / sum)));
		}
	}

	public void increment(String key, int value) {
		if (this.containsKey(key)) {
			this.put(
					new Text(key),
					new IntWritable(((IntWritable) this.get(key)).get() + value));
		} else {
			this.put(new Text(key), new IntWritable(value));
		}
	}

	@Override
	public String toString() {
		int counter = 1;
		StringBuilder buffer = new StringBuilder("<");
		for (Writable key : keySet()) {
			buffer.append("(").append(key).append(",").append(get(key))
					.append(")");
			if (counter != keySet().size())
				buffer.append(", ");
			counter++;
		}
		return buffer.append(">").toString();
	}
}