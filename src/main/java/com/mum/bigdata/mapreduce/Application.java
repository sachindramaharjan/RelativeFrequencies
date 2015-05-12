package com.mum.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.mum.bigdata.util.Pair;

public class Application {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [method] [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Application.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		switch (args[0].toLowerCase()) {
		case "pairs":
			job.setNumReduceTasks(2);

			job.setMapperClass(PairsMapper.class);
			job.setPartitionerClass(PairsPartitioner.class);
			job.setReducerClass(PairsReducer.class);

			job.setMapOutputKeyClass(Pair.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Pair.class);
			job.setOutputValueClass(IntWritable.class);

			break;

		case "stripes":

			break;

		case "hybrid":

			break;

		default:
			break;

		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
