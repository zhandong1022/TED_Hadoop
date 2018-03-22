package jpk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 70423 on 2018/3/22.
 */
public class MaxTemperatureReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text, IntWritable>{
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MAX_VALUE;
        while(iterator.hasNext()){
            maxValue = Math.max(maxValue,iterator.next().get());
        }
        outputCollector.collect(text,new IntWritable(maxValue));
    }
}
