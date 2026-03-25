package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FriendsOfFriends {

    public static class FriendsOfFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("[()]", "").trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length == 3) {
                String p1 = parts[0].trim();
                String p2 = parts[1].trim();
                String weight = parts[2].trim();
                
                context.write(new Text(p1), new Text(p2 + ":" + weight));
                context.write(new Text(p2), new Text(p1 + ":" + weight));
            }
        }
    }

    public static class FriendsOfFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> friends = new ArrayList<>();
            List<Double> weights = new ArrayList<>();
            
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                friends.add(parts[0]);
                weights.add(Double.parseDouble(parts[1]));
            }
            
            for (int i = 0; i < friends.size(); i++) {
                for (int j = i + 1; j < friends.size(); j++) {
                    String f1 = friends.get(i);
                    String f2 = friends.get(j);
                    double combinedWeight = weights.get(i) + weights.get(j);
                    
                    context.write(new Text(f1 + "," + f2), new Text(String.valueOf(combinedWeight)));
                    context.write(new Text(f2 + "," + f1), new Text(String.valueOf(combinedWeight)));
                }
            }
        }
    }

    public static class AggregateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private String filterPerson = null;
        
        @Override
        protected void setup(Context context) {
            filterPerson = context.getConfiguration().get("filter.person");
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String pair = parts[0].trim();
                double weight = Double.parseDouble(parts[1].trim());
                
                // If filter is set, only emit pairs where the first person in the pair matches
                if (filterPerson != null) {
                    String[] pairParts = pair.split(",");
                    if (pairParts.length == 2 && !pairParts[0].trim().equals(filterPerson)) {
                        return; // Skip this pair
                    }
                }
                context.write(new Text(pair), new DoubleWritable(weight));
            }
        }
    }

    public static class AggregateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            sum = Math.round(sum * 100.0) / 100.0;
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: FriendsOfFriends <in_path> <out_path> [person_id]");
            System.err.println("  person_id (optional): Filter results for a specific person (e.g., P1)");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Force local framework configuration to avoid the "Cannot initialize Cluster" remote server error
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // Set hadoop.home.dir to prevent Windows bin directory issues
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        
        // Set optional filter person
        if (args.length == 3) {
            conf.set("filter.person", args[2]);
        }

        Path intermediatePath = new Path(args[1] + "_temp");

        Job job1 = Job.getInstance(conf, "FriendsOfFriends - Step 1");
        job1.setJarByClass(FriendsOfFriends.class);
        job1.setMapperClass(FriendsOfFriendsMapper.class);
        job1.setReducerClass(FriendsOfFriendsReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        boolean success = job1.waitForCompletion(true);
        if (success) {
            Job job2 = Job.getInstance(conf, "FriendsOfFriends - Step 2");
            job2.setJarByClass(FriendsOfFriends.class);
            job2.setMapperClass(AggregateMapper.class);
            job2.setReducerClass(AggregateReducer.class);
            
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job2, intermediatePath);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
