import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MatrixMultiplication {
    public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: movie_B \t movie_A=ratio
            // output: < key=movie_B, value="movie_A=ratio" >
            String[] tokens = value.toString().split("\t");
            context.write(new Text(tokens[0]), new Text(tokens[1]));

        }
    }

    public static class RatingMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: userID, movieID, rating
            // output: < key=movie_B, value="userID: rating">
            String[] tokens = value.toString().split(",");
            String movie = tokens[1].trim();
            String userID = tokens[0].trim();
            String rating = tokens[2].trim();
            context.write(new Text(movie), new Text(userID + ":" + rating));
        }
    }

    public static class  MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        // hashMap for user average rating
        public Map<String, Double> user_AverageMap = new HashMap<String, Double>();

        @Override
        public void setup(Context context) throws IOException {
            // read the userID and average rating, save to a hashMap
            Configuration configuration = context.getConfiguration();
            String fileName = configuration.get("fileName", "/averageRating");
            fileName = fileName + "/part-r-*";

            // build the file path in HDFS
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path path = new Path(fileName);
            FileStatus[] statusList = fileSystem.globStatus(path);

            BufferedReader br;
            String line;

            // read average rating record
            for (FileStatus status: statusList) {
                br = new BufferedReader(new InputStreamReader(fileSystem.open(status.getPath())));
                line = br.readLine();

                while (line != null) {
                    // userID \t averageRating
                    String[] userRating = line.split("\t");
                    String userID = userRating[0].trim();
                    double rating = Double.parseDouble(userRating[1].trim());
                    user_AverageMap.put(userID, rating);

                    line = br.readLine();
                }
                br.close();
            }
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input: < key=movie_B, value="movie_A=ratio1, movie_C=ratio2, ..., user1: rating1, user2: rating2, ..." >
            // output: < key="userID: movieID", value=ratio * rating >
            Map<String, Double> movie_CorrMap = new HashMap<String, Double>();
            Map<String, Double> user_RatingMap = new HashMap<String, Double>();

            for (Text value: values){
                if (value.toString().contains("=")){
                    String[] movie_corr = value.toString().trim().split("=");
                    movie_CorrMap.put(movie_corr[0].trim(), Double.parseDouble(movie_corr[1]));
                }else{
                    String[] user_rating = value.toString().trim().split(":");
                    user_RatingMap.put(user_rating[0].trim(), Double.parseDouble(user_rating[1]));
                }
            }

            for (Map.Entry<String,Double> entry1: movie_CorrMap.entrySet()){
                String movie = entry1.getKey();
                double corr = entry1.getValue();

                for (Map.Entry<String,Double> entry2: user_AverageMap.entrySet()){
                    String user = entry2.getKey();
                    double rating;
                    if (user_RatingMap.containsKey(user)){
                        rating = user_RatingMap.get(user);
                    }else{
                        rating = entry2.getValue();
                    }
                    context.write(new Text(user + ":" + movie), new DoubleWritable(corr*rating));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        // args[0]: user average rating folder, e.g: /averageRating
        // args[1]: normalized covariance matrix folder, e.g.: /Normalize
        // args[2]: original user rating folder, e.g., /input
        // args[3]: matrix multiplication output, e.g., /Multiplication

        Configuration conf = new Configuration();
        conf.set("fileName", args[0]);
        Job job = Job.getInstance(conf, "Matrix Multiplication");


        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MultiplicationMapper.class);
        job.setMapperClass(RatingMatrixMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MultiplicationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMatrixMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
    }
 }
