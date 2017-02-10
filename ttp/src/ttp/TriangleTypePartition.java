package ttp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.SecondarySort.IntPair;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;


public class TriangleTypePartition extends Configured implements Tool {
	 /* Το πρόγραμμα διαβάζει δεδομένα από ενα αρχείο το αρχείο έχει της ακμές του γράφου
		 * πρέπει να είναι της μορφής για κάθε ακμή (v,u) θα πρέπει να υπάρχουν δύο γραμμές στο
		 * αρχείο v u και u v 
		 * πχ για τις ακμές (a,b) και (c,a) θα είχαμε το παρακάτω αρχείο
		 * a b
		 * b a
		 * c a
		 * a c
		 * 
		 * */
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new TriangleTypePartition(), args);
        System.exit(res);
		
	}
	
	public int run(String[] args) throws Exception{
		
		long startTime = System.nanoTime();
		getConf().setInt("partitions", Integer.parseInt(args[2]));
		
		Job job1 = new Job(getConf());
		job1.setJobName("count");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp"));
		job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntPair.class);
		job1.setJarByClass(TriangleTypePartition.class);
		job1.setMapperClass(Map_ex_1.class);
		job1.setReducerClass(Red_ex_1.class);
		
		Job job2 = new Job(getConf());
		job2.setJobName("sum");
		FileInputFormat.setInputPaths(job2, new Path("temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setJarByClass(TriangleTypePartition.class);
		job2.setMapOutputKeyClass(ByteWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(Map_ex_2.class);
		job2.setReducerClass(Red_ex_2.class);

		int ret = job1.waitForCompletion(true) ? 0 : 1;
		if (ret == 0)
			ret = job2.waitForCompletion(true) ? 0 : 1;
		long estimatedTime = System.nanoTime() - startTime; // Χρόνο εκτέλεσης
		System.out.println("Estimated Execution Time = " + estimatedTime
				+ " nanoseconds");
		System.out.println("Estimated Execution Time = " + estimatedTime
				/ 1000000000 + " seconds");
		System.exit(ret);
		return ret;
	}

}
