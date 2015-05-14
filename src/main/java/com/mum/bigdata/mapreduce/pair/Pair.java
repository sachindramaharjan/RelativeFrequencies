package com.mum.bigdata.mapreduce.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	Text first;
	Text second;

	public Pair() {
		this.first = new Text();
		this.second = new Text();
	}

	public void set(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}

	public void set(String str) {
		String[] s = str.split(",");
		this.first.set(s[0]);
		this.second.set(s[1]);
	}

	public String getKey() {
		return first + "," + second;
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(Pair other) {
		int cmp = this.first.compareTo(other.getFirst());
		if (cmp != 0) {
			return cmp;
		} else if (this.second.toString().equals("*")) {
			return -1;
		} else if (other.getSecond().toString().equals("*")) {
			return 1;
		}

		return this.second.compareTo(other.getSecond());
	}

	@Override
	public String toString() {
		return "Pair(" + first + "," + second + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Pair) {
			Pair pair = (Pair) o;
			return (first.equals(pair.first) && second.equals(pair.second));
		}
		return true;
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
}
