import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

class Pair{
    String key;
    double value;
    Pair(String k, double v){
        this.key = k;
        this.value = v;
    }
}

public class Recommend {
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: userID: movieID \t rating
            // output: < key="userID", value = "movieID":rating >

            String[] tokens = value.toString().trim().split("\t|\\:");
            context.write(new Text(tokens[0].trim()), new Text(tokens[1] + ":" + tokens[2]));
        }
    }

    public static class RatedMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: userID, movieID, rating
            // output: < key=userID, value=movieID>
            String[] tokens = value.toString().split(",");
            String movie = tokens[1].trim();
            String userID = tokens[0].trim();
            context.write(new Text(movie), new Text(userID));
        }
    }


    public static class RecommendReducer extends Reducer<Text, Text, Text, Text> {
        private PriorityQueue<Pair> queue = null;
        private int k;

        private Comparator<Pair> pairComparator = new Comparator<Pair>() {
            public int compare(Pair left, Pair right) {
                if (Double.compare(left.value, right.value) != 0) {
                    return Double.compare(left.value, right.value);
                }
                return right.key.compareTo(left.key);
            }
        };

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("number", 5);
            this.queue = new PriorityQueue<Pair>(k, pairComparator);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input: < key="userID"> value=< movieID1:Rating1, movieID2:Rating2, ...movieRatedID1, movieRatedID2 > >
            // output: < key="userID" value = "movieID1,movieID2..(top k movies)" >
            Map<String, Double> ratingMap = new HashMap<String, Double>();
            Set<String> ratedMovies = new HashSet<String>();
            for (Text value : values) {
                if (value.toString().contains(":")) {
                    String[] tokens = value.toString().split(":");
                    String movie = tokens[0].trim();
                    double rating = Double.parseDouble(tokens[1].trim());
                    ratingMap.put(movie, rating);
                } else {
                    ratedMovies.add(value.toString().trim());
                }
            }

            for (Map.Entry<String, Double> entry : ratingMap.entrySet()) {
                String movie = entry.getKey();
                double rating = entry.getValue();
                Pair pair = new Pair(movie, rating);
                if (ratedMovies.contains(movie)) {
                    continue;
                }
                if (queue.size() < k) {
                    queue.add(pair);
                } else {
                    Pair pair1 = queue.peek();
                    if (pairComparator.compare(pair, pair1) > 0) {
                        queue.poll();
                        queue.add(pair);
                    }
                }
            }

            List<Pair> pairs = new ArrayList<Pair>();
            while (!queue.isEmpty()) {
                pairs.add(queue.poll());
            }

            int size = pairs.size();
            for (int i = size - 1; i >= 0; i--) {
                Pair pair = pairs.get(i);
                context.write(key, new Text(pair.key + "\t" + pair.value));
            }


        }

    }

    public static void main(String[] args) throws Exception {

        // args[0]: matrix multiplication sum output folder, e.g., /Sum
        // args[1]: input folder, e.g., /input
        // args[2]: recommend output , e.g /recommend_output
        // args[3]: num of recommendations


        Configuration conf = new Configuration();
        conf.set("k", args[3]);


        Job job = Job.getInstance(conf);
        job.setJarByClass(Recommend.class);

        job.setMapperClass(Recommend.RatingMapper.class);
        job.setMapperClass(Recommend.RatedMapper.class);
        job.setReducerClass(Recommend.RecommendReducer.class);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Recommend.RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Recommend.RatedMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));


        job.waitForCompletion(true);
    }
}
