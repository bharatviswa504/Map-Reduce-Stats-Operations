/**
 * Created by Bharat on 10/1/2015.
 */

import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StatsReducer extends
        Reducer<Text, Text, Text, Text> {
    ArrayList<Integer> numList = new ArrayList<Integer>();
    int med;
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        int median = 0;
        int mean=0;
        for (Text value : values) {
            numList.add(Integer.parseInt(value.toString()));
            mean+=Integer.parseInt(value.toString());
        }

        Collections.sort(numList);

        HashMap hm = new HashMap();
        for(int i=0;i<numList.size();i++)
        {
            if(!hm.containsKey(numList.get(i)))
            {
                hm.put(numList.get(i),1);
            }
            else {
                int count = Integer.parseInt(hm.get(numList.get(i)).toString());
                hm.put(numList.get(i), ++count);
            }
        }

        Set set = hm.entrySet();
        // Get an iterator
        Iterator i = set.iterator();

        int largestRepeated=0;
        int largestCount=0;

        // Display elements
        while(i.hasNext()) {
            Map.Entry me = (Map.Entry)i.next();
          //  System.out.print(me.getKey() + ": ");
            //System.out.println(me.getValue());
            if(Integer.parseInt(me.getValue().toString()) >largestRepeated)
            {
                largestRepeated=Integer.parseInt(me.getValue().toString());
                largestCount=Integer.parseInt(me.getKey().toString());
            }
        }

        String key3Value="Mode";
        Text val3 = new Text();
        val3.set(Integer.toString(largestCount));

        context.write(new Text(key3Value),val3);
        int size  = numList.size();
        mean=mean/size;
        if(size%2 == 0){
            int half = size/2;
            median  = (numList.get(half) + numList.get(half-1))/2;

        }else {
            int half = (size + 1)/2;
            median = numList.get(half -1);
        }


        String key2="Median";
        String key1="Mean";

        String key1Value = Integer.toString(mean);
        String key2Value = Integer.toString(median);

        Text val1 = new Text();
        val1.set(key1Value);

        Text val2 = new Text();
        val2.set(key2Value);


        context.write(new Text(key2), val2);
        context.write(new Text(key1), val1);
    }
}

