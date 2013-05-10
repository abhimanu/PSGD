/**
 *	This just reads the input file and splits up into d keys and corressponding vals
 *	this leads to running of d independent reducers
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

public class PSGDCombinerMapper extends MapReduceBase implements Mapper<Text, Text, IntWritable, FloatArray> {
//    int d = 1;	// not needed
	
	Random randomGen = new Random();


	public void configure(JobConf job) {


		//d = job.getInt("psgd.d", 1);

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
		int k = 0; 
		float val = 0;
		try {
			i = (int)(vals[0].charAt(0)); 
			j = Integer.parseInt(vals[1]); 
			k = Integer.parseInt(vals[2]); 

			val = Float.parseFloat(vals[3]);
		} catch (Exception e) {
			System.out.println("Error on input: ");
			System.out.println("Key: " + key.toString());
			System.out.println("Value: " + value.toString());
			return;
		}

//		int keyVal = randomGen.nextInt(d)+1;
		IntWritable newkey = new IntWritable(i);
		FloatArray newvalue = new FloatArray(new float[]{j,k,val});

		output.collect(newkey, newvalue);
		reporter.incrCounter("PSGD", "Number Passed", 1);

		reporter.incrCounter("PSGD", "Number Total", 1);

	}

}

