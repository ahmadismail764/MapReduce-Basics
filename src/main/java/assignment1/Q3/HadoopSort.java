package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class HadoopSort {

  
    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey   = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {

            String text = line.toString().trim();
            if (text.isEmpty()) {
                return;
            }

            // Support both TAB-separated and space-separated key/value pairs
            String[] parts;
            if (text.contains("\t")) {
                parts = text.split("\t", 2);
            } else {
                parts = text.split("\\s+", 2);
            }

            if (parts.length == 2) {
                outKey.set(parts[0].trim());
                outVal.set(parts[1].trim());
            } else {
                // Treat the whole line as the key with an empty value
                outKey.set(text);
                outVal.set("");
            }

            context.write(outKey, outVal);
        }
    }

    
    public static class SortReducer
            extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                outVal.set(val.toString());
                context.write(key, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: HadoopSort <input_path> <output_path>");
            System.err.println("Example: HadoopSort input/ output/");
            System.err.println("Note: Output directory must not exist before running the job.");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // Use a single reducer so all keys end up in one sorted output file
        Job job = Job.getInstance(conf, "Hadoop Sort by Key");
        job.setJarByClass(HadoopSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
