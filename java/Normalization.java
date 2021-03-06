import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Normalization {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: movie_A: movie_B \t count
            // output: < key=movie_A, value="movie_B=count" >
            String[] input = value.toString().split(":|\\\t");
            context.write(new Text(input[0].trim()),new Text(input[1].trim()+"="+input[2].trim()));

        }
    }

    public static class  NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input: < key=movie_A, value=< movie_B=count, movie_C=count, ... > >
            // output: < key=movie_B, value="movieA=count/total" >
            int total = 0;
            String movie_A = key.toString();
            Map<String, Integer> movie_count = new HashMap<String, Integer>();
            for(Text value : values){
               String movie_B = value.toString().split("=")[0];
               int count = Integer.parseInt(value.toString().split("=")[1].trim());
               total += count;
               movie_count.put(movie_B, count);

            }

            for(Map.Entry<String, Integer> entry: movie_count.entrySet()){

                String movie_B = entry.getKey();
                int count = entry.getValue();
                double weight = (double)count/total;
                context.write(new Text(movie_B), new Text(movie_A+"="+weight));

            }

        }
    }

    public static void main(String[] args) throws Exception {

        // args[0]: un-normalized cooccurrence matrix folder, e.g., /coOccurrenceMatrix
        // args[1]: normalized cooccurrence matrix folder, e.g., /Normalize

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Normalize Cooccurrence Matrix");

        job.setJarByClass(Normalization.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
