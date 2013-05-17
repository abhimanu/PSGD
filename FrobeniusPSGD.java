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

public class FrobeniusPSGD extends Configured implements Tool  {

	static long long_multiplier = 100000;
	
	class FrobeniusPSGDMapper extends MapReduceBase implements Mapper<Text, Text, IntWritable, FloatArray> {
    int d = 0;
	
	Random randomGen = new Random();


	public void configure(JobConf job) {


		d = job.getInt("psgd.d", 1);
		System.out.println("Mapper d "+d);

	}

	public void map(Text key, Text value, OutputCollector<IntWritable,FloatArray> output, Reporter reporter) throws IOException {

		//System.out.println("Key: " + key.toString());
		//System.out.println("Value: " + value.toString());
		String[] vals1 = key.toString().split("\\s+");
		String[] vals2 = value.toString().split("\\s+");
		String[] vals = new String[vals1.length + vals2.length];
		int cnt = 0;
		for(int i = 0; i < vals1.length; i++) {
			vals[cnt] = vals1[i];
			cnt++;
		}
		for(int i = 0; i < vals2.length; i++) {
			vals[cnt] = vals2[i];
			cnt++;
		}
		
		// Load from key/values
		int i = 0; 
		int j = 0; 
//		int k = 0; 
		float val = 0;
		try {
			i = Integer.parseInt(vals[0]); 
			j = Integer.parseInt(vals[1]); 

			val = Float.parseFloat(vals[2]);
//			if(!is2D) {
//				k = Integer.parseInt(vals[2]);
//				val = Float.parseFloat(vals[3]); 
//   		}
		} catch (Exception e) {
			System.out.println("Error on input: ");
			System.out.println("Key: " + key.toString());
			System.out.println("Value: " + value.toString());
			return;
		}

		int keyVal = 1;					// make the key fixed so that every thing goes to the single reducer
		IntWritable newkey = new IntWritable(keyVal);
		FloatArray newvalue = new FloatArray(new float[]{i,j,val});

		output.collect(newkey, newvalue);
		reporter.incrCounter("PSGD", "Number Passed", 1);
//		}

		reporter.incrCounter("PSGD", "Number Total", 1);

	}

}
	public static class FrobeniusPSGDReducer extends MapReduceBase implements Reducer<IntWritable, FloatArray, NullWritable, NullWritable> {
		DenseTensor U;
		DenseTensor V;

		int rank;

		int N;
		int M;

		int d;

        //long long_multiplier = 100000;


		double step_size = 0.000001;
		int dataSets = 1;
		JobConf thisjob;

		String outputPath; 
		String prevPath;
		boolean KL = false;
		boolean lda_simplex = true;

		boolean debug = false;

		public void configure(JobConf job) {

			thisjob = job;

			outputPath = job.getStrings("psgd.outputPath")[0];
			prevPath = job.getStrings("psgd.prevPath", new String[]{""})[0];

			KL = (job.getInt("psgd.KL",0) == 1);

			lda_simplex = (job.getInt("psgd.lda_simplex",0) == 1);




			d = job.getInt("psgd.d", 1);
			rank = job.getInt("psgd.rank", 1);
			N = job.getInt("psgd.N", 1);

			System.out.println("d,rank,N:" + d + ", " + rank + ", " + N );

			M = job.getInt("psgd.M", 1);

			System.out.println("M  " + M);
			
			V = new DenseTensor(M,rank);

			U = new DenseTensor(N,rank);			// Note N is the main dimension
			
			step_size = job.getFloat("psgd.stepSize",0.000001f);

			

		}


		public void reduce ( final IntWritable key, final Iterator<FloatArray> values, final OutputCollector<NullWritable, NullWritable> output, final Reporter reporter) throws IOException { 

			System.out.println("Key: " + key.toString());
            //Get U and V values
			ReaderWriterClass rwClass = new ReaderWriterClass();
			if(prevPath!=""){
				rwClass.getGlobalValue('U', U, thisjob, prevPath);  // this method only read prev global vals
				rwClass.getGlobalValue('V', V, thisjob, prevPath);
				System.out.println("Frobenius Reading from previous U and V");
			}

			int numSoFar = 0;
			int curSubepoch = -99999;
			int ci = -1;
			int cj = -1;
			int ck = -1;
			while(values.hasNext()) {	// run SGD for U

				FloatArray v = values.next();

				int i = (int)(v.ar[0]);
				int j = (int)(v.ar[1]);
				float val = v.ar[2];



				getLoss(i,j,val,reporter);
				reporter.progress();

			}
			System.out.println("Last batch: " + numSoFar);

		}

		float pertubation = 0.0001f;
		public void getLoss(int i, int j, float val, Reporter reporter) {
			float eval = 0;
			//String s = "";
			for(int r = 0; r < rank; r++) {
				float prod = U.get(i,r) * V.get(j,r);
				//s += U.get(i,r) + "*" +  V[dataSet].get(j,r);
				eval += prod;
				//s += " + ";
			}

			//System.err.println("Difference: " + val + "\t" +  eval + "\t" + s);
			float loss = (float)Math.pow(val - eval, 2);
			if(KL) { 
				if(eval == 0)
					eval = pertubation;
				loss = (float)(val*Math.log(val/eval)); 
			}
			long loss_estimate = (long)Math.round(loss * long_multiplier);
			reporter.incrCounter("PSGD Loss","Loss", loss_estimate);
//			reporter.incrCounter("PSGD Loss","Loss-data"+dataSet, loss_estimate);
			reporter.incrCounter("PSGD Loss","Points", 1);
//			reporter.incrCounter("psgd Loss","Points-data"+dataSet, 1);
		}


	}


	public int run (String[] args) throws Exception {
		for(int i = 0; i < args.length; i++){
			System.out.println(i + " : " + args[i]);
		}

		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] <d> <maxDimensions> <dataSets> <key> <input> <input2?> <output> <prevrun?> \n",
					getClass().getSimpleName()); ToolRunner.printGenericCommandUsage(System.err); 
			return -1;
		}

		int d = Integer.parseInt(args[0]);

		String jobName = args[1] ;

		int iter = 1;
		for(int i = 0; i < iter; i++) {
			System.out.println("Sub-iteration " + i);

			JobConf conf = getJobInstance(jobName);
			FileSystem fs = FileSystem.get(conf);

			conf.setInt("psgd.d", d);
			conf.setInt("psgd.subepoch", i);

			FileInputFormat.addInputPath(conf, new Path(args[2])); 
			conf.setStrings("psgd.outputPath", args[3]);
			conf.setStrings("psgd.prevPath", args[4]);

			RunningJob job = JobClient.runJob(conf);

			//if(i % d == 0) {
			long loss = job.getCounters().findCounter("PSGD Loss","Loss").getCounter();
			long count = job.getCounters().findCounter("PSGD Loss","Points").getCounter();
			long updated = job.getCounters().findCounter("PSGD","Number Passed").getCounter();

			String key = "-" + jobName;//args[3];

			BufferedWriter lossResults = new BufferedWriter(new FileWriter("/h/abhimank/loss_PSGD" + key + ".txt",true)); ;
			//BufferedWriter lossResults = new BufferedWriter(new FileWriter("/home/abeutel/loss" + key + ".txt",true)); ;
			//BufferedWriter lossResults = new BufferedWriter(new FileWriter("~/loss" + key + ".txt",true)); ;
				lossResults.write(i + "\t" + loss +"\t" + (loss*1.0f/long_multiplier) + "\t" + count + "\t" + updated + "\t" + ((loss*1.0f/long_multiplier)/count)+ "\t" + Math.sqrt( (loss*1.0f/long_multiplier)/count ) + "\n");
			lossResults.close();
			//}
		}	

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

	public JobConf getJobInstance(String sub) {
		JobConf conf = new JobConf(getConf(), FrobeniusPSGD.class); 
		conf.setJobName("FrobeniusPSGD-"+sub);

		conf.setMapperClass(FrobeniusPSGDMapper.class); 
		//conf.setCombinerClass(psgdReducer.class); 
		conf.setReducerClass(FrobeniusPSGDReducer.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.setOutputFormat(TensorMultipleOutputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(FloatArray.class);

//		conf.setPartitionerClass(psgdPartitioner.class);
//		conf.setOutputKeyComparatorClass(KeyComparator.class);
//		conf.setOutputValueGroupingComparator(GroupComparator.class);


		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(Text.class);
		conf.setNumReduceTasks(1);				// constant number of 1 reducer

		return conf;
	}


	/**
	 * Required parameters:
	 * psgd.subepoch (possibly set in run() with a loop
	 * psgd.N size of input matrix 
	 * psgd.M size of input matrix
	 * psgd.d number of blocks to do in parallel (number of reducers) 
	 * psgd.rank rank of matrix (dimension of U and V)
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FrobeniusPSGD(), args);
		System.exit(exitCode); 
	}

}
	


