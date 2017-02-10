package ttp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Iterator;
import org.apache.hadoop.examples.SecondarySort.IntPair;

/*
* First Reducer
*/
public class Red_ex_1 extends Reducer <Text, IntPair, Text, DoubleWritable> {

	
	
	public void reduce(Text _key, Iterable <IntPair> values,
			Context context) throws IOException, InterruptedException {
		ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);

		Iterator<IntPair> it = values.iterator();
		int p;
		myGraph g= new myGraph();//cration of the graph
		Configuration conf = context.getConfiguration();
		p = conf.getInt("partitions", -1);
		
		while(it.hasNext()) {
			IntPair edge=it.next();
			g.addEdge(edge.getFirst(), edge.getSecond());//add edge at the graph
		}
	 	
		context.write(null,new DoubleWritable(g.countTrianglesCompFor(p)));//compute the number of triangles
		
	}
	}

