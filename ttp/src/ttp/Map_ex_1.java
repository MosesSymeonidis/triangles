package ttp;
            
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.examples.SecondarySort.IntPair;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
	

/**
* First mapper
*/
public class Map_ex_1 extends Mapper <LongWritable, Text, Text, IntPair> {
		
	
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		
		//read of the edge
		String line = value.toString();
		IntPair valueOut = new IntPair();
		StringTokenizer tokenizer = new StringTokenizer(line);
		int u = 0, v = 0;
		if (tokenizer.hasMoreTokens()) {
			u = Integer.parseInt(tokenizer.nextToken());
			if (!tokenizer.hasMoreTokens())
				throw new RuntimeException("invalid edge line " + line);
			v = Integer.parseInt(tokenizer.nextToken());
		}
		
		
		int p, a, b, c;
		Configuration conf = context.getConfiguration();
		p = conf.getInt("partitions", -1);//parameter p the number of partitions

		if (u < v) {

			for (a = 0; a < p - 1; a++) {

				for (b = a + 1; b < p; b++) {
					//compute and send 2-partitions
					if (((a == u % p) && (v % p == b))
							|| ((a == v % p) && (u % p == b))
							|| ((a == v % p) && (u % p == a))
							|| ((b == v % p) && (u % p == b))) {
						valueOut.set(u, v);
						context.write(new Text(a + "," + b + ",-1"), valueOut);
					}
					/*copmute and send 3-partitions
					*/
					
					if (u % p != v % p && b < p - 1) {
						for (c = b + 1; c < p; c++) {
							if (((u % p == a) && (v % p == c))
									|| ((v % p == a) && (u % p == c))
									|| ((a == u % p) && (v % p == b))
									|| ((a == v % p) && (u % p == b))
									|| ((c == u % p) && (v % p == b))
									|| ((c == v % p) && (u % p == b))) {
								valueOut.set(u, v);
								context.write(new Text(a+","+ b+","+c), valueOut);
							}
						}
					}
				}
			}
			
	      /* 
	       * The original code of the paper
	       if (u%p!=v%p){
	        	 for (a=0;a<p-2;a++){
	 	        	for (b=a+1;b<p-1;b++){
	 	        		for (c=b+1;c<p;c++){
	 	        			if(((u%p==a)&&(v%p==c))||((v%p==a)&&(u%p==c))||((a==u%p)&&(v%p==b))||((a==v%p)&&(u%p==b))||((c==u%p)&&(v%p==b))||((c==v%p)&&(u%p==b))){
	 	        				valueOut.set(u,v);
	 		        			context.write(new IntTriple(a,b,c) ,valueOut);
	 	        			}
	 	        		}
	 	        	}
	 	        }
			}*/
	        
		}
		}

}
