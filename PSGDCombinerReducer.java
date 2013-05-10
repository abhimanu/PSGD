/**
 * PSGD Implementation 
 *
 */
import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class PSGDCombinerReducer extends MapReduceBase implements Reducer<IntWritable, FloatArray, NullWritable, NullWritable> {

	int dPrev;
	int M;
	int N;
	int rank;
//    float stepSize;
	
	DenseTensor U,V;


	JobConf thisjob;

	String outputPath; 
	String prevPath;

	String taskId;


	public void configure(JobConf job) {
		//System.out.println("TEST");

		thisjob = job;

		outputPath = job.getStrings("psgd.outputPath")[0];					// contains till runi/data/
		prevPath = job.getStrings("psgd.prevPath", new String[]{""})[0];	// dont need this


		dPrev = job.getInt("psgd.dPrev", 1);
		M = job.getInt("psgd.M", 1);		// first dimension
		N = job.getInt("psgd.N", 1);		// second dimension
		rank = job.getInt("psgd.rank",1);
//		stepSize = job.getFloat("psgd.stepSize",0.00001f);

		U = new DenseTensor(M,rank,1,0);	// TODO: Dont reset, wastage of time
		V = new DenseTensor(N,rank,1,0);	// TODO:


		taskId = getAttemptId(job);
	}

	public static String getAttemptId(Configuration conf) { // throws IllegalArgumentException {
		if (conf == null) {
			return "";
			//throw new NullPointerException("conf is null");
		}

		String taskId = conf.get("mapred.task.id");
		if (taskId == null) {
			return "";
			//throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
		}

		String[] parts = taskId.split("_");
		//if (parts.length != 6 || !parts[0].equals("attempt") || (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
			//throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
		//}
		return parts[parts.length - 1];		//TODO: What is going on here

		//return parts[4] + "-" + parts[5];
	}


//	Format of the paths are ..../runi/datad/U.id V.id
//	For final its  ..../runi/data/U.id V.id

	/*
	 * get previous values
	 * I only get path till ..../runi in prevPath
	 * prevPath always exist since in the very begining alll have to start from same U0 V0
	 *
	*/

//	public boolean getPreviousValue(char c, DenseTensor T, JobConf thisjob, String prevPath)

    /*
	 *
	 * Written as c + "" + "\t" + i + "\t" + j + "\t" + val
	 *
	 */

//	public void readInFactors(FSDataInputStream in, DenseTensor T, char c) throws IOException


	/*
	*/


	public void reduce (
		final IntWritable key, 
		final Iterator<FloatArray> values, 
		final OutputCollector<NullWritable, NullWritable> output, 
		final Reporter reporter
	) throws IOException { 

		System.out.println("Key: " + (char)key.get());

		DenseTensor T;
		
		char c = (char) key.get();
		if(c=='U')
			T = U;
		else
			T = V;


//		FileSystem fs = FileSystem.get(thisjob);
//		String path  = outputPath + "/data/"+"log."+taskId;
//		FSDataOutputStream out = fs.create(new Path(path));

        // read previous global U and V
		ReaderWriterClass rwClass = new ReaderWriterClass();
		if(prevPath!=""){
			rwClass.getPreviousValue('U', U, thisjob, prevPath);	// this method only read prev global vals
			rwClass.getPreviousValue('V', V, thisjob, prevPath);
		}
		
		while(values.hasNext()) {	// write the partitioned data

			FloatArray v = values.next();
//			System.out.println("Key: " + key.toString()+ "   Vals: " + v.toString());
			


			int i = (int)(v.ar[0]);
			int j = (int)(v.ar[1]);
			float val = v.ar[2];

			if(Float.isNaN(val) || Float.isInfinite(val) || Math.abs(val) > 10) { 
				System.out.print("val NaN: ");
				System.out.println(v.toString());
			}
			
//			String valStr = i+"\t"+j+"\t"+val+"\n";

//			out.writeBytes(valStr);

//			out.writeBytes(valStr);


			reporter.progress();

			T.set(i,j,T.get(i,j)+val);


			reporter.incrCounter("PSGD", "Number Processed", 1);
		}
		
        // Nomalize the values
//		float val = T.get(key.ar[1],key.ar[2])*1.0/dPrev;
//		T.set(key.ar[1],key.ar[2],val);

        for(int i=0; i<T.N; i++){
			for(int j=0; j<T.M; j++){
				float val = T.get(i,j)*1.0f/dPrev;
				T.set(i,j,val);
			}
			reporter.progress();
		}

		System.out.println("Last batch: " );
//		Write Every thing in the end
		String writePath = outputPath;			// outputPath contains runi/data
		rwClass.writeFactors(c, T, thisjob, writePath, taskId, reporter);

//		fs.close();



	}


}




/*
 *
 *
 *
 *	Junk	Junk	Junk	Junk
 *
 *
 *
 *
 *
 */


