package GP;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.examples.SecondarySort.IntPair;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map_ex_1 extends Mapper<LongWritable, Text, Text, IntPair> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		/*
		 * Ο πρώτος Mapper διαβάζει δεδομένα από ενα αρχείο το αρχείο έχει της
		 * ακμές του γράφου πρέπει να είναι της μορφής για κάθε ακμή (v,u) θα
		 * πρέπει να υπάρχουν δύο γραμμές στο αρχείο v u και u v πχ για τις
		 * ακμές (a,b) και (c,a) θα είχαμε το παρακάτω αρχείο a b b a c a a c
		 */

		// Διάβασμα ακμής
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
		p = conf.getInt("partitions", -1);// παίρνει την παράμετρο p του
											// αλγορίθμου δηλαδή τον αριθμό των
											// partitions

		// εκτέλεση αλγορίθμου για το Map
		//

		if (u < v) {
			int i = u % p;
			int j = v % p;
			for (a = 0; a < p; a++) {
				for (b = a + 1; b < p; b++) {
					for (c = b + 1; c < p; c++) {
						//ο αλγόριθμος στέλνει όλα τα 3-partition
						if (((i == a) && (j == c))
								|| ((j == a) && (i == c))
								|| ((a == i) && (j == b))
								|| ((a == j) && (i == b))
								|| ((c == i) && (j == b))
								|| ((c == j) && (i == b)) || (c == j)
								&& (i == c) || (b == j) && (i == b) || (a == j)
								&& (i == a)) {
							valueOut.set(u, v);
							context.write(new Text(a+","+b+","+c), valueOut);
						}

					}
				}
			}
		}

	}

}
