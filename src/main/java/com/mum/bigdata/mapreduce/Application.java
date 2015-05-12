package com.mum.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mum.bigdata.mapreduce.pair.Pair;
import com.mum.bigdata.mapreduce.pair.PairsMapper;
import com.mum.bigdata.mapreduce.pair.PairsPartitioner;
import com.mum.bigdata.mapreduce.pair.PairsReducer;
import com.mum.bigdata.mapreduce.stripes.Stripe;
import com.mum.bigdata.mapreduce.stripes.StripeMapper;
import com.mum.bigdata.mapreduce.stripes.StripeReducer;

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
		case "pair":
			job.setNumReduceTasks(2);

			job.setMapperClass(PairsMapper.class);
			job.setPartitionerClass(PairsPartitioner.class);
			job.setReducerClass(PairsReducer.class);

			job.setMapOutputKeyClass(Pair.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Pair.class);
			job.setOutputValueClass(IntWritable.class);

			break;

		case "stripe":
			job.setMapperClass(StripeMapper.class);
			job.setReducerClass(StripeReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Stripe.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Stripe.class);

			break;

		case "hybrid":

			break;

		default:
			System.out.println("method: [pair/stripe/hybrid]");
			System.exit(-1);
			break;

		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
