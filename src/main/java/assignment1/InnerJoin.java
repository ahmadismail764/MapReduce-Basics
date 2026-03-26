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
    private static final String TABLE_1 = "T1";
    private static final String TABLE_2 = "T2";
    private static final String SEP = "|";

    public static class InnerJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

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

        private void emit(Context context, String joinKey, String sourceTag, String payload)
                throws IOException, InterruptedException {
            outKey.set(joinKey);
            outVal.set(sourceTag + SEP + payload);
            context.write(outKey, outVal);
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

            if (TABLE_1.equalsIgnoreCase(table)) {
                // key = A1, value carries T1 payload A2
                emit(context, pk, TABLE_1, attr);
                return;
            }

            if (TABLE_2.equalsIgnoreCase(table)) {
                // key = A1 (foreign key in T2), value carries T2 payload A3
                emit(context, attr, TABLE_2, pk);
            }
        }
    }

    public static class InnerJoinReducer extends Reducer<Text, Text, Text, Text> {
        private String[] splitTaggedVal(String tagged) {
            String[] parts = tagged.split("\\" + SEP, 2);
            return parts.length == 2 ? parts : null;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> t1A2Vals = new ArrayList<>();
            List<String> t2A3Vals = new ArrayList<>();

            for (Text value : values) {
                String[] parts = splitTaggedVal(value.toString());
                if (parts == null) {
                    continue;
                }

                if (TABLE_1.equals(parts[0])) {
                    t1A2Vals.add(parts[1]);
                } else if (TABLE_2.equals(parts[0])) {
                    t2A3Vals.add(parts[1]);
                }
            }

            if (t1A2Vals.isEmpty() || t2A3Vals.isEmpty()) {
                return;
            }

            // Inner join emits only matched keys.
            for (String a3 : t2A3Vals) {
                for (String a2 : t1A2Vals) {
                    context.write(new Text(a3), new Text(key.toString() + "," + a2));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InnerJoin <in_path> <out_path>");
            System.err.println("Example: InnerJoin input/ output/");
            System.err.println("Note: Output directory must not exist before running the job.");
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
