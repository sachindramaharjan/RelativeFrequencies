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

	public int baseCompareTo(Pair other) {
		int cmp = first.compareTo(other.getFirst());
		return cmp;
	}

	@Override
	public String toString() {
		return "Pair(" + first + ", " + second + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Pair pair = (Pair) o;
		if (second != null ? !second.equals(pair.second) : pair.second != null)
			return false;
		if (first != null ? !first.equals(pair.first) : pair.first != null)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = first != null ? first.hashCode() : 0;
		result = 163 * result + (second != null ? second.hashCode() : 0);
		return result;
	}
}
