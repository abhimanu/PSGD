/**
 * Based on that paper (insert link)
 *
 */
import java.io.*;
import java.util.*;
import java.text.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class PSGDCombiner extends Configured implements Tool  {

	public int run (String[] args) throws Exception {
		for(int i = 0; i < args.length; i++){
			System.out.println(i + " : " + args[i]);
		}

        if (args.length < 2) {
			System.err.printf("Usage: %s [Hadoop Options] <d> <maxDimensions> <dataSets> <key> <input> <input2?> <output> <prevrun?> \n"
					+ "Required Hadoop Options:\n"
					+ "dsgd.N=# Number of columns (or rows, whatever the first dimension is) for the primary matrix or tensor.  (This dimension will be shared with coupled data.)\n"
					+ "dsgd.M0=# Range of second dimension in 1st data set\n"
					+ "dsgd.rank=# Rank of the decomposition\n"
					+ "dsgd.stepSize=# Step size for SGD.  This is typically 1/N where N is the number of non-zero elements\n"
					+ "mapred.reduce.tasks=# This should be set to the value of d so that the number of reducers matches the parallelism of the problem precisely\n\n"
					+ "Optional Hadoop Options:\n"
					+ "dsgd.P0=# Range of third dimension in 1st data set\n"
					+ "dsgd.M1=# Range of second dimension in 2nd data set\n"
					+ "dsgd.P1=# Range of third dimension in 2nd data set\n"
					+ "dsgd.debug=1 - If set to 1 will use plain text files and will be more verbose\n\n",
					getClass().getSimpleName()); ToolRunner.printGenericCommandUsage(System.err); 
			return -1;
		}

/*
 *
 *
 *
 *		d=1		// just one reducer for combining the results
 *		args[0] = dprev	//previous number of reducers
 *		args[1] = job-name
 *		args[2] = input-path
 *		args[3] = output-path
 *		args[4] = prev-path
 *
 *
 *
 *
 */		

//		int d = Integer.parseInt(args[0]);		// directly passed as -D psgd.d= argument
//		int rank = Integer.parseInt(args[1]);



		int dPrev = Integer.parseInt(args[0]);
		JobConf conf = getJobInstance(args[1], 1);		// just one reducer
		FileSystem fs = FileSystem.get(conf);

		conf.setInt("psgd.dPrev", dPrev);

		for(int i=1; i<=dPrev; i++)						// assuming that input path is runi <= is necesary 
			FileInputFormat.addInputPath(conf, new Path(args[2]+"/data"+i)); 

		conf.setStrings("psgd.outputPath", args[3]);	// this output path will be .../runi/data

//		Previous path not needed for combiner.

//		if(args.length>4)
//			conf.setStrings("psgd.prevPath", args[4]);
//		else
//			conf.setStrings("psgd.prevPath","");

		RunningJob job = JobClient.runJob(conf);


		return 0;
	}


	public void addFilesToCache(String path, FileSystem fs, JobConf conf) throws Exception {

		if(fs.exists(new Path(path))) {
			FileStatus[] Vfiles = fs.listStatus(new Path(path));
			for(FileStatus f : Vfiles) {
				DistributedCache.addCacheFile(f.getPath().toUri(), conf);
			}
		}

	}

	public JobConf getJobInstance(String sub, int d) {
		JobConf conf = new JobConf(getConf(), PSGDCombiner.class); 
		conf.setJobName("PSGD-"+sub);

//		if(!isPaired) conf.setMapperClass(DSGDMapper.class); 
		conf.setMapperClass(PSGDCombinerMapper.class); 
		conf.setReducerClass(PSGDCombinerReducer.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.setOutputFormat(TensorMultipleOutputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(FloatArray.class);

//		conf.setPartitionerClass(PSGDPartitioner.class);
//		conf.setOutputKeyComparatorClass(KeyComparator.class);
//		conf.setOutputValueGroupingComparator(GroupComparator.class);


		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(Text.class);

		conf.setNumReduceTasks(d);				// d is always passed as 1

		return conf;
	}


	/**
	 * Required parameters:
	 * dsgd.subepoch (possibly set in run() with a loop
	 * dsgd.N size of input matrix 
	 * dsgd.M size of input matrix
	 * dsgd.d number of blocks to do in parallel (number of reducers) 
	 * dsgd.rank rank of matrix (dimension of U and V)
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PSGDCombiner(), args);
		System.exit(exitCode); 
	}

}
