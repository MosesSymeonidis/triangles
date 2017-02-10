package GP;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.SecondarySort.IntPair;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;


public class GraphPartition extends Configured implements Tool {
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new GraphPartition(), args);
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
		job1.setJarByClass(GraphPartition.class);
		job1.setMapperClass(Map_ex_1.class);
		job1.setReducerClass(Red_ex_1.class);
		
		Job job2 = new Job(getConf());
		job2.setJobName("sum");
		FileInputFormat.setInputPaths(job2, new Path("temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setJarByClass(GraphPartition.class);
		job2.setMapOutputKeyClass(ByteWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(Map_ex_2.class);
		job2.setReducerClass(Red_ex_2.class);

		int ret = job1.waitForCompletion(true) ? 0 : 1;
		if (ret == 0)
			ret = job2.waitForCompletion(true) ? 0 : 1;
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("Estimated Execution Time = " + estimatedTime
				+ " nanoseconds");
		System.out.println("Estimated Execution Time = " + estimatedTime
				/ 1000000000 + " seconds");
		System.exit(ret);
		return ret;
	}

}
