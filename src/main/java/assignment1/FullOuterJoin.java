package assignment1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class FullOuterJoin {

    public static class FullOuterJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Write mapping logic here
        }
    }

    public static class FullOuterJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Write reduce logic here
        }
    }

    public static void main(String[] args) throws Exception {
        // Job configuration logic
    }
}
