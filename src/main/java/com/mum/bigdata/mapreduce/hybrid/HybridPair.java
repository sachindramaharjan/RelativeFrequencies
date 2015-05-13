package com.mum.bigdata.mapreduce.hybrid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class HybridPair implements WritableComparable<HybridPair> {
	private Text first;
	private Text second;

	public HybridPair() {
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
	public int compareTo(HybridPair other) {
		int cmp = first.compareTo(other.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(other.getSecond());
		}
		return cmp;
	}

	public int baseCompareTo(HybridPair other) {
		int cmp = first.compareTo(other.getFirst());
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null || !(obj instanceof HybridPair)) {
			return false;
		}
		return this.equals((HybridPair) obj);
	}

	public int baseHashCode() {
		return Math.abs(first.hashCode());
	}

	@Override
	public int hashCode() {
		int result = first != null ? first.hashCode() : 0;
		result = 163 * result + (second != null ? second.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Pair(" + first + ", " + second + ")";
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

}
