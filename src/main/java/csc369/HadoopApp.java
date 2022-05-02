package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("AverageTemperature".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AverageTemperature.ReducerImpl.class);
	    job.setMapperClass(AverageTemperature.MapperImpl.class);
            job.setCombinerClass(AverageTemperature.CombinerImpl.class);
	    job.setOutputKeyClass(AverageTemperature.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AverageTemperature.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("TemperatureSort".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(TemperatureSort.ReducerImpl.class);
	    job.setMapperClass(TemperatureSort.MapperImpl.class);
            
            job.setPartitionerClass(TemperatureSort.PartitionerImpl.class);
            job.setGroupingComparatorClass(TemperatureSort.GroupingComparator.class);
            job.setSortComparatorClass(TemperatureSort.SortComparator.class);
            
	    job.setOutputKeyClass(TemperatureSort.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(TemperatureSort.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
