package com.mum.bigdata.mapreduce.util;

import java.text.DecimalFormat;

public class Utility {
	public static double formatNumber(double value) {
		DecimalFormat df = new DecimalFormat("###.000");
		double freq = Double.parseDouble(df.format(value));
		return freq;
	}

}
