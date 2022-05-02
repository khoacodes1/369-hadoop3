package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class TemperatureSort {

    public static final Class OUTPUT_KEY_CLASS = YMTemperaturePair.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // read a text file that contains (y, m, d, temperature) readings
    public static class MapperImpl extends Mapper<LongWritable, Text, YMTemperaturePair, IntWritable> {
          @Override
          public void map(LongWritable key, Text value, Context
                          context) throws IOException, InterruptedException {
              String line = value.toString();
              String[] tokens = line.split(",");
              String yearMonth = tokens[0].trim() +"-"+
                  tokens[1].trim();
              int temperature = Integer.parseInt(tokens[3].trim());
              context.write(new YMTemperaturePair(yearMonth, temperature), new IntWritable(temperature));
          }
    }
    

    // controls the reducer to which a particular (key, value) is sent
    public static class PartitionerImpl extends Partitioner<YMTemperaturePair, IntWritable> {
        @Override
        public int getPartition(YMTemperaturePair pair,
                                IntWritable temperature,
                                int numberOfPartitions) {
            return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
        }
    }
    
    // used to group (year,month,day) data by (year,month)
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(YMTemperaturePair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            YMTemperaturePair pair = (YMTemperaturePair) wc1;
            YMTemperaturePair pair2 = (YMTemperaturePair) wc2;
            return pair.getYearMonth().compareTo(pair2.getYearMonth());
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(YMTemperaturePair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            YMTemperaturePair pair = (YMTemperaturePair) wc1;
            YMTemperaturePair pair2 = (YMTemperaturePair) wc2;
            return pair.compareTo(pair2);
        }
    }
    
    // output one line for each month, with the temperatures sorted for that month
    public static class ReducerImpl extends Reducer<YMTemperaturePair, IntWritable, Text, Text> {

            @Override
            protected void reduce(YMTemperaturePair key,
                                  Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                String result="";
                for (IntWritable value : values) {
                    result += (value.toString()+",");
                }
                result = result.substring(0, result.length()-1);
                context.write(key.getYearMonth(), new Text(result));
            }
    }

}
