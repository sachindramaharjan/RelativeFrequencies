package com.mum.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairsMethod implements RelativeFrequency{
	private String inputPath, outputPath;
	Application context;
	
	public PairsMethod(String inputPath, String outputPath, Application context) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		
	}

	public void execute() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(PairsMapper.class);
		job.setReducerClass(PairsReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(Application.class);
		job.submit();
	}

}
