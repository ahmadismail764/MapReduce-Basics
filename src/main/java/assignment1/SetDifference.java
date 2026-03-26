
package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SetDifference {

    public static class SetDifferenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private String[] parseRecord(String line) {
            String cleaned = line.replaceAll("[()]", "").trim();
            if (cleaned.isEmpty()) {
                return null;
            }
            String[] parts = cleaned.split(",");
            if (parts.length != 3) {
                return null;
            }
            return new String[] { parts[0].trim(), parts[1].trim(), parts[2].trim() };
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = parseRecord(value.toString());
            if (record == null) {
                return;
            }

            String table = record[0];
            String pk = record[1];
            String attr = record[2];

            if ("T1".equalsIgnoreCase(table)) {
                outKey.set(pk);
                outValue.set("T1");
                context.write(outKey, outValue);
            } else if ("T2".equalsIgnoreCase(table)) {
                // In T2(A3, A1), the third field stores A1.
                outKey.set(attr);
                outValue.set("T2");
                context.write(outKey, outValue);
            }
        }
    }

    public static class SetDifferenceReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean inT1 = false;
            boolean inT2 = false;

            for (Text value : values) {
                String source = value.toString();
                if ("T1".equals(source)) {
                    inT1 = true;
                } else if ("T2".equals(source)) {
                    inT2 = true;
                }
            }

            if (inT1 && !inT2) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: SetDifference <in_path> <out_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.app-submission.cross-platform", "true");



        Job job = Job.getInstance(conf, "Set Difference A1[T1] - A1[T2]");
        job.setJarByClass(SetDifference.class);
        job.setMapperClass(SetDifferenceMapper.class);
        job.setReducerClass(SetDifferenceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
