package khoacodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CountryCount {
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
        private Map<String, String> country_map = new HashMap<>();
        private Text country = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            URI[] cacheFiles = context.getCacheFiles();
            FileReader file_path = new FileReader(cacheFiles[0].getPath());

            BufferedReader reader = new BufferedReader(file_path);

            String line;
            while ((line = reader.readLine()) != null)
            {
                line = line.trim();
                String[] parts = line.split(","); // 0x503e4fce.virnxx2.adsl-dhcp.tele.dk , Denmark
                String hostname = parts[0].trim();
                String countryName = parts[1].trim();
                country_map.put(hostname, countryName);
            }

            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String logLine = value.toString().trim();

            ApacheLogParse parser = new ApacheLogParse();

            if (parser.parse(logLine))
            {
                String hostname = parser.getHost();
                String countryName = country_map.get(hostname);

                if (countryName != null) {
                    country.set(countryName);
                    context.write(country, one);
}
            }
        }
    }

    // ("Canada", [1, 1]), ("United States", [1, 1, 1, 1])
    public static class CountrySumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // ("Canada", 2)
        }
    }

    public static class CountryCountComparator extends WritableComparator{
        protected CountryCountComparator() 
        {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable val1 = (IntWritable) a;
            IntWritable val2 = (IntWritable) b;
            return -1 * val1.compareTo(val2);
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        private IntWritable count = new IntWritable();
        private Text country = new Text();

        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString().trim();
            String[] parts = line.split("\\t");

            country.set(parts[0]);
            count.set(Integer.parseInt(parts[1]));
            context.write(count, country);

        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text country : values) {
                context.write(country, key);
            }
        }
    }

    public static boolean run(String inputLog, String inputCSV, String tempOutput, String finalOutput) throws Exception
    {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Task1 - Country Request Count");
        job1.setJarByClass(CountryCount.class);

        Path csvPath = new Path(inputCSV);
        if (!csvPath.isAbsolute()) {
            csvPath = new Path(System.getProperty("user.dir"), inputCSV);
        }

        job1.addCacheFile(csvPath.toUri());
        job1.setMapperClass(CountryMapper.class);
        job1.setReducerClass(CountrySumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(inputLog));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Sort By Count Descending");
        job2.setJarByClass(CountryCount.class);
        
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(CountryCountComparator.class);
        
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));

        return job2.waitForCompletion(true);

    }

}

