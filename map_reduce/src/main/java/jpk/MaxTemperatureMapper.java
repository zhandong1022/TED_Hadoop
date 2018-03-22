package jpk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by 70423 on 2018/3/22.
 */
public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        String line = text.toString();
        String year = line.substring(15,19);
        int airTemperature;
        if(line.charAt(64)=='+'){
            airTemperature=Integer.parseInt(line.substring(81,95));
        }else{
            airTemperature = 1;
        }
        outputCollector.collect(new Text(year),new IntWritable(airTemperature));

    }
}
