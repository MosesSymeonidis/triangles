package ttp;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Iterator;
	
public class Red_ex_2 extends Reducer <ByteWritable, DoubleWritable, LongWritable, DoubleWritable> {

	
	
	public void reduce(ByteWritable _key, Iterable <DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
	
		Iterator<DoubleWritable> it = values.iterator();
		double Sum=0.0;
		
		while(it.hasNext()) {//αθροίζω τις τιμές
		Sum+=it.next().get();
		
			
		}
	 	
		context.write(null,new DoubleWritable(Sum));//αποθηκεύουμε το άθροισμα στο δίσκο
		
	}
	}

