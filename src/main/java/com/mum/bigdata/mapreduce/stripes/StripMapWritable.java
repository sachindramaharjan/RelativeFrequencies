package com.mum.bigdata.mapreduce.stripes;

import java.io.IOException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StripMapWritable extends MapWritable {
	public StripMapWritable() {
		super();
	}

	public static StripMapWritable create(DataInputBuffer in)
			throws IOException {
		StripMapWritable m = new StripMapWritable();
		m.readFields(in);

		return m;
	}

	public void plus(StripMapWritable m) {
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

	// to increment the value
	public void increment(String key) {
		increment(key, 1);
	}

	public void increment(String key, int n) {
		if (this.containsKey(key)) {
			this.put(new Text(key),
					new IntWritable(((IntWritable) this.get(key)).get() + n));
		} else {
			this.put(new Text(key), new IntWritable(n));
		}
	}
}
