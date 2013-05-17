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

public class PSGDSplitterReducer extends MapReduceBase implements Reducer<IntWritable, FloatArray, NullWritable, NullWritable> {

	int d;
	int M;
	int N;
	int rank;
    float stepSize;
	
	DenseTensor U,V;


	JobConf thisjob;

	String outputPath; 
	String prevPath;

	String taskId;


	public void configure(JobConf job) {
		//System.out.println("TEST");

		thisjob = job;

		outputPath = job.getStrings("psgd.outputPath")[0];					// contains till runi
		prevPath = job.getStrings("psgd.prevPath", new String[]{""})[0];	// contains till runi



		d = job.getInt("psgd.d", 1);
		M = job.getInt("psgd.M", 1);		// second dimension
		N = job.getInt("psgd.N", 1);		// first dimension
		rank = job.getInt("psgd.rank",1);
		stepSize = job.getFloat("psgd.stepSize",0.00001f);

		System.out.println("d M N rank stepSize"+" "+M+" "+N+" "+rank+" "+stepSize);



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

//	public boolean getGlobalValue(char c, DenseTensor T, JobConf thisjob, String prevPath)

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

		System.out.println("Key: " + key.toString());
		U = new DenseTensor(N,rank);	// TODO: Dont reset, wastage of time
		V = new DenseTensor(M,rank);	// TODO:

//		FileSystem fs = FileSystem.get(thisjob);
//		String path  = outputPath + "/data/"+"log."+taskId;
//		FSDataOutputStream out = fs.create(new Path(path));

        // read previous global U and V
		ReaderWriterClass rwClass = new ReaderWriterClass();
		if(prevPath!=""){
			rwClass.getGlobalValue('U', U, thisjob, prevPath);	// this method only read prev global vals
			rwClass.getGlobalValue('V', V, thisjob, prevPath);
			System.out.println("Splitter reading from previous U and V");
		}
		
		while(values.hasNext()) {	// write the partitioned data

			FloatArray v = values.next();

			int i = (int)(v.ar[0]);
			int j = (int)(v.ar[1]);
			float val = v.ar[2];

			if(Float.isNaN(val) || Float.isInfinite(val) || Math.abs(val) > 10) { 
				System.out.print("val NaN: ");
				System.out.println(v.toString());
			}
			
			String valStr = i+"\t"+j+"\t"+val+"\n";

//			out.writeBytes(valStr);

			float coeff = getGradient(i,j,val);
//			out.writeBytes(valStr);

			float[] U_i = new float[rank];
			float[] V_j = new float[rank];

   		for(int r=0;r<rank;r++){
				U_i[r] = U.get(i,r);
				V_j[r] = V.get(j,r);
			}

			reporter.progress();

			for(int r=0;r<rank;r++){
				setGradient(U,i,r, coeff, V_j);
				setGradient(V,j,r, coeff, U_i);
			}



			reporter.incrCounter("PSGD", "Number Processed", 1);
		}

		System.out.println("Last batch: " );
//		Write Every thing in the end
		String writePath = outputPath + "/data"+key.toString();
		rwClass.writeFactors('U', U, thisjob, writePath, taskId, reporter);
		rwClass.writeFactors('V', V, thisjob, writePath, taskId, reporter);

//		fs.close();



	}

	private float getGradient(int i, int j, float val){
		float sum = 0;
		for(int r=0;r<rank;r++) {
			float prod = U.get(i, r)*V.get(j, r); 
			sum+=prod;
		}
		if(Float.isNaN(sum) || Float.isInfinite(sum)){
			System.out.println("getGradient sum NaN: " + sum);
		}
		return -2.0f*(val-sum);
	}


	private void setGradient(DenseTensor M, int i, int r, double coeff, float[] M1) {
		float newVal = (float)(M.get(i, r) - stepSize * coeff * M1[r] );
		
		if(Float.isNaN(newVal) || Float.isInfinite(newVal)) { 
			System.out.print("newVal NaN: ");
			System.out.println(i + ", " + r + ", " + coeff + ": " + newVal);
		}

//		if(sparse) {
//			newVal = softThreshold(newVal,lambda * stepSize);
//		}

		M.set(i, r, newVal);
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


