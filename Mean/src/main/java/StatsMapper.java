/**
 * Created by Bharat on 10/1/2015.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatsMapper  extends Mapper<LongWritable, Text, Text, Text> {
    public Text yearText = new Text();
    public Text nm = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String line = value.toString();
        nm.set(line);
        context.write(new Text("1"),nm);
    }



}
