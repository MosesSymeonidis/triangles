package GP;
            
import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
	


public class Map_ex_2 extends Mapper <LongWritable, Text, ByteWritable, DoubleWritable> {
		
	
	ByteWritable b= new ByteWritable(Byte.parseByte("1"));
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		//στη δεύτερη φάση απλά διαβάζουμε τα επιμέρους αρθοίσματα 
		//του προηγούμενου βήματος και τα στέλνουμε όλα σε ένα reducer
		//για το συνολικό άθροισμα
		 String line = value.toString();
		 double t=Double.parseDouble(line);
         DoubleWritable v =new DoubleWritable(t);
            if (t>0.0)
            {
            	context.write(b ,v);
                }
		
	}

}
