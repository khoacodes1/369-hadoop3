package khoacodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.Arrays;

public class URLCountryList
{
    public static class URLCountryMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private Map<String, String> country_map = new HashMap<>();
        private Text urlKey = new Text();
        private Text countryValue = new Text();

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
                String[] parts = line.split(",");
                String hostname = parts[0].trim();
                String countryName = parts[1].trim();
                country_map.put(hostname, countryName);
            }

            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            ApacheLogParse parser = new ApacheLogParse();

            if (parser.parse(value.toString()))
            {
                String hostname = parser.getHost();
                String url = parser.getUrl();
                String urlNoQuery = stripQueryParams(url);
                String countryName = country_map.get(hostname);

                urlKey.set(urlNoQuery);
                countryValue.set(countryName);
                context.write(urlKey, countryValue);
                
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

    public static class URLCountryReducer extends Reducer<Text, Text, Text, Text>
    {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Map<String, Boolean> uniqueCountries = new HashMap<>();

            for (Text val : values)
            {
                uniqueCountries.put(val.toString(), true);
            }

            String[] countriesArray = uniqueCountries.keySet().toArray(new String[0]);
            
            Arrays.sort(countriesArray);

            StringBuilder countryList = new StringBuilder();
            for (int i = 0; i < countriesArray.length; i++) {
                if (i > 0) {
                    countryList.append(", ");
                }
                countryList.append(countriesArray[i]);
            }

            result.set(countryList.toString());
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            int tabIndex = line.indexOf('\t');
           
            String url = line.substring(0, tabIndex);
            String countries = line.substring(tabIndex + 1);
            context.write(new Text(url), new Text(countries));
            
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }
    

    public static boolean run(String inputLog, String inputCSV, String tempOutput, String finalOutput) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tempPath = new Path(tempOutput);
        Path finalPath = new Path(finalOutput);
      
        Job job1 = Job.getInstance(conf, "Task3 - URL Country Aggregation");
        job1.setJarByClass(URLCountryList.class);

        Path csvPath = new Path(inputCSV);

        job1.addCacheFile(csvPath.toUri());
        job1.setMapperClass(URLCountryMapper.class);
        job1.setReducerClass(URLCountryReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(inputLog));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            return false;
        }
        Job job2 = Job.getInstance(conf, "Task3 - Sort by URL");
        job2.setJarByClass(URLCountryList.class);
        
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));

        return job2.waitForCompletion(true);
    }




}
