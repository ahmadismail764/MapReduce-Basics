package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class InnerJoin {

    public static class InnerJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
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
                // key = A1, value carries T1 payload A2
                outKey.set(pk);
                outValue.set("T1|" + attr);
                context.write(outKey, outValue);
            } else if ("T2".equalsIgnoreCase(table)) {
                // key = A1 (foreign key in T2), value carries T2 payload A3
                outKey.set(attr);
                outValue.set("T2|" + pk);
                context.write(outKey, outValue);
            }
        }
    }

    public static class InnerJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> t1A2Values = new ArrayList<>();
            List<String> t2A3Values = new ArrayList<>();

            for (Text value : values) {
                String text = value.toString();
                String[] parts = text.split("\\|", 2);
                if (parts.length != 2) {
                    continue;
                }

                if ("T1".equals(parts[0])) {
                    t1A2Values.add(parts[1]);
                } else if ("T2".equals(parts[0])) {
                    t2A3Values.add(parts[1]);
                }
            }

            if (t1A2Values.isEmpty() || t2A3Values.isEmpty()) {
                return;
            }

            // Inner join emits only matched keys.
            for (String a3 : t2A3Values) {
                for (String a2 : t1A2Values) {
                    context.write(new Text(a3), new Text(key.toString() + "," + a2));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InnerJoin <in_path> <out_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "Inner Join T1 and T2");
        job.setJarByClass(InnerJoin.class);
        job.setMapperClass(InnerJoinMapper.class);
        job.setReducerClass(InnerJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
