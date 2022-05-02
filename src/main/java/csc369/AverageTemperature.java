package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageTemperature {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = SumCountPair.class;

    // read a text file that contains (date, temperature) pairs -- multiple temperature readings per day
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, SumCountPair> {
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String valueAsString = value.toString().trim();
            String[] tokens = valueAsString.split(" ");
            if (tokens.length != 2) {
                return;
            }
            context.write(new Text(tokens[0]), new SumCountPair(Integer.parseInt(tokens[1]),1));
        }
    }

    // combiner executes on map nodes as a "mini-reducer"  (combines data based on key, before it is sent to reducer notes)
    public static class CombinerImpl extends Reducer<Text, SumCountPair, Text, SumCountPair> {
        @Override
        public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (SumCountPair el : values) {
                sum += el.getSum();
                count += el.getCount();
            }
            context.write(key, new SumCountPair(sum,count));
        }
    }
    

    // compute average temperature for each day
    public static class ReducerImpl extends Reducer<Text, SumCountPair, Text, DoubleWritable> {
        
        @Override
        public void reduce(Text date, Iterable<SumCountPair> temperatures, Context context)
            throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for(SumCountPair el: temperatures){
                sum += el.getSum();
                count += el.getCount();
            }
            context.write(date, new DoubleWritable(sum/count));
        }
    }    
    
}
