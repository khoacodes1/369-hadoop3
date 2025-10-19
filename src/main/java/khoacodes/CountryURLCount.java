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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CountryURLCount
{
    public static class CountryURLKey implements WritableComparable<CountryURLKey> {
        private String country;
        private int count;
        private String url;

        public CountryURLKey()
        {
            this.country = "";
            this.url = "";
            this.count = 0;
        }

        public CountryURLKey(String country, int count, String url) 
        {
            this.country = country;
            this.count = count;
            this.url = url;
        }

        public String getCountry() { 
            return country; 
        }
        
        public int getCount() { 
            return count; 
        }
        
        public String getUrl() { 
            return url; 
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(country);
            out.writeInt(count);
            out.writeUTF(url);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            country = in.readUTF();
            count = in.readInt();
            url = in.readUTF();
        }

        @Override
        public int compareTo(CountryURLKey other)
        {
            int countryCompare = this.country.compareTo(other.country);
            if (countryCompare != 0) {
                return countryCompare;
            }
            
            int countCompare = Integer.compare(other.count, this.count);
            if (countCompare != 0) {
                return countCompare;
            }
            
            return this.url.compareTo(other.url);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CountryURLKey)) return false;
            CountryURLKey that = (CountryURLKey) o;
            return country.equals(that.country) && 
                   count == that.count && 
                   url.equals(that.url);
        }

        @Override
        public int hashCode() {
            return country.hashCode();
        }

        @Override
        public String toString() {
            return country + "," + count + "," + url;
        }

    }

    public static class CountryURLMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private Map<String, String> country_map = new HashMap<>();
        private final IntWritable one = new IntWritable(1);
        private Text countryURL = new Text();
        private ApacheLogParse parser = new ApacheLogParse();

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

            if (parser.parse(value.toString()))
            {
                String hostname = parser.getHost();
                String url = parser.getUrl();
                String urlNoQuery = stripQueryParams(url);
                String countryName = country_map.get(hostname);

                if (countryName != null && urlNoQuery != null && !urlNoQuery.isEmpty()) {
                    countryURL.set(countryName + "\t" + urlNoQuery);
                    context.write(countryURL, one);
                }

            }            
        }

        private String stripQueryParams(String url) {
            if (url == null) return null;
            int queryStart = url.indexOf('?');
            if (queryStart >= 0) {
                return url.substring(0, queryStart);
            }
            return url;
        }

    }

    public static class CountryURLReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;

            for (IntWritable val : values)
            {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, CountryURLKey, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return; 
        }

        String[] parts = line.split("\t", 3);
        String country = parts[0];
        String url = parts[1];
        int count = Integer.parseInt(parts[2].trim());

        CountryURLKey compositeKey = new CountryURLKey(country, count, url);
        context.write(compositeKey, new Text(line));
    }
}

    public static class CountryPartitioner extends Partitioner<CountryURLKey, Text> {
        @Override
        public int getPartition(CountryURLKey key, Text value, int numPartitions) {
            return (key.getCountry().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CountryGroupComparator extends WritableComparator {
        protected CountryGroupComparator() {
            super(CountryURLKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CountryURLKey k1 = (CountryURLKey) a;
            CountryURLKey k2 = (CountryURLKey) b;
            return k1.getCountry().compareTo(k2.getCountry());
        }
    }


    public static class SortReducer extends Reducer<CountryURLKey, Text, Text, IntWritable> {
        
        @Override
        protected void reduce(CountryURLKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length == 3) {
                    String country = parts[0];
                    String url = parts[1];
                    int count = Integer.parseInt(parts[2]);
                    
                    context.write(new Text(country + "\t" + url), new IntWritable(count));
                }
            }
        }
    }

    public static boolean run(String inputLog, String inputCSV, String tempOutput, String finalOutput) throws Exception {
        Configuration conf = new Configuration();
        
        Job job1 = Job.getInstance(conf, "Task2");
        job1.setJarByClass(CountryURLCount.class);

        Path csvPath = new Path(inputCSV);

        job1.addCacheFile(csvPath.toUri());
        job1.setMapperClass(CountryURLMapper.class);
        job1.setReducerClass(CountryURLReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(inputLog));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            return false;
        }

        Job job2 = Job.getInstance(conf, "Task2");
        job2.setJarByClass(CountryURLCount.class);
        
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setPartitionerClass(CountryPartitioner.class);
        job2.setGroupingComparatorClass(CountryGroupComparator.class);

        job2.setMapOutputKeyClass(CountryURLKey.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));

        return job2.waitForCompletion(true);
    }

}