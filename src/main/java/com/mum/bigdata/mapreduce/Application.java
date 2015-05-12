package com.mum.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mum.bigdata.util.Pair;

public class Application extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Application(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
	        System.out.println("usage: [input] [output]");
	        System.exit(-1);
	    }
		
		Job job = Job.getInstance(new Configuration());
	    
	    job.setMapperClass(PairsMapper.class);
	    job.setReducerClass(PairsReducer.class);

	    job.setMapOutputKeyClass(Pair.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(IntWritable.class);
	  
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setJarByClass(Application.class);
	    job.submit();
	    return 0;
	}

    
	

//	@Override
//	public int run(String[] args) throws Exception {
//		if (args.length != 3) {
//			System.out.println("usage: [method] [input] [output]");
//			System.exit(-1);
//		}
//
//		switch (args[0].toLowerCase()) {
//		case "pairs":
//			RelativeFrequency method = new PairsMethod(args[1], args[2], this);
//			method.execute();
//			break;
//
//		case "stripes":
//
//			break;
//		case "hybrid":
//
//			break;
//
//		default:
//			break;
//
//		}
//
//		return 0;
//	}

}
