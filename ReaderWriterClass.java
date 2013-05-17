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

public class ReaderWriterClass {



//	Format of the paths are ..../runi/datad/U.id V.id
//	For final its  ..../runi/data/U.id V.id

	/*
	 * get previous values
	 * I only get path till ..../runi in prevPath
	 * prevPath always exist since in the very begining alll have to start from same U0 V0
	 *
	*/
	
	// This method only reads previosu global vals
	public boolean getGlobalValue(char c, DenseTensor T, JobConf thisjob, String prevPath) throws IOException{
		FileSystem fs = FileSystem.get(thisjob);
		String path = prevPath + "/data/"+c;
		System.out.println("In getGlobalValue");
		FileStatus[] allFiles = fs.globStatus(new Path(path+".*"));   
		if(allFiles!=null && allFiles.length >0){
			for(FileStatus f: allFiles){
				try{
					FSDataInputStream in = fs.open(f.getPath());
					readInFactors(in,T,c);
					fs.close();
					return true;
				}catch(EOFException e){
					System.out.println("EOFexception");
					continue;
				}catch(IOException e){
					System.out.println("Error Reading factors");
					continue;
				}
			}
		}
		System.out.println("Error Reading factors");
		fs.close();
		return false;	// It will fail for the starting task for the time being, 
	}					// It's fien coz Tensor is intialized by random no.s but we want to change and start same



    /*
	 *
	 * Written as c + "\t" + i + "\t" + j + "\t" + val
	 *
	 */


	public void readInFactors(FSDataInputStream in, DenseTensor T, char c) throws IOException {
		Scanner s = new Scanner(in);
		while(s.hasNext()){
			String key = s.next();		
			int i = s.nextInt();
			int j = s.nextInt();
			float val = s.nextFloat();
			if(key.toString().charAt(0)==c){
				T.set(i,j,val);
			}else{
				System.out.println("Error reading input, Mismatch on factors.");
			}		
		}	
	}


	/*
	*/


	public void writeFactors(char c, DenseTensor T, JobConf thisjob, String writePath, String taskId, final Reporter reporter) throws IOException {

		//System.out.println("WRITE FACTOR: " + c + index + ", " + iter);
		FileSystem fs = FileSystem.get(thisjob);

//		String fp = outputPath + "/iter" + iter + "/" + c;

//		Path path = new Path(writePath);
//		if (!fs.exists(path)) {
//			fs.mkdirs(path);
//		}

		writePath += "/" + c + "." + taskId;
		FSDataOutputStream out = fs.create(new Path(writePath));

		System.out.println("Write to " + writePath);
		for(int i = 0; i < T.N; i++) {
			for(int j = 0; j < T.M; j++) {
				String val = c + "\t" + i + "\t" + j + "\t" + T.get(i,j) + "\n";
				out.writeBytes(val);
			}
		}

//		writeLog(c,index,iter,fs);		// should we write on log as well?
		fs.close();

	}


}
