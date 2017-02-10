package ttp;
            
    import java.io.IOException;
import java.util.StringTokenizer;
   import org.apache.hadoop.examples.SecondarySort.IntPair;
  
import org.apache.hadoop.conf.Configuration;
   // import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
	


public class Map_ex_1 extends Mapper <LongWritable, Text, Text, IntPair> {
		
	
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		/* Ο πρώτος Mapper διαβάζει δεδομένα από ενα αρχείο το αρχείο έχει της ακμές του γράφου
		 * πρέπει να είναι της μορφής για κάθε ακμή (v,u) θα πρέπει να υπάρχουν δύο γραμμές στο
		 * αρχείο v u και u v 
		 * πχ για τις ακμές (a,b) και (c,a) θα είχαμε το παρακάτω αρχείο
		 * a b
		 * b a
		 * c a
		 * a c
		 * 
		 * */
		//Διάβασμα ακμής
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
		p = conf.getInt("partitions", -1);//παίρνει την παράμετρο p του αλγορίθμου δηλαδή τον αριθμό των partitions

		//εκτέλεση αλγορίθμου για το Map
		//
		if (u < v) {

			for (a = 0; a < p - 1; a++) {

				for (b = a + 1; b < p; b++) {
					//Υπολογισμός και αποστολή 2-partitions
					if (((a == u % p) && (v % p == b))
							|| ((a == v % p) && (u % p == b))
							|| ((a == v % p) && (u % p == a))
							|| ((b == v % p) && (u % p == b))) {
						valueOut.set(u, v);
						context.write(new Text(a + "," + b + ",-1"), valueOut);
					}
					/*υπολογισμός και αποστολή 3-partitions
					*ο υπολογισμός για τα 3-partitions στο paper 
					*γίνεται με τον κώδικα παρακάτω που βρίσκεται 
					*σε σχόλια για να μην κάνουμε 2 for και να 
					*μειώσουμε τον χρόνο εκτέλεσης στέλνουμε και
					*τα 3-partitions στο ίδιο for
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
	       * κώδικας όπως περιγράφεται στο paper
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
