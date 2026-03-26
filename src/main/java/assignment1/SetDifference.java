
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
import java.util.HashSet;
import java.util.Set;

public class SetDifference {
    private static final String TABLE_1 = "T1";
    private static final String TABLE_2 = "T2";

    public static class SetDifferenceMapper extends Mapper<LongWritable, Text, Text, Text> {
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

        private void emit(Context context, String key, String sourceTag) throws IOException, InterruptedException {
            outKey.set(key);
            outVal.set(sourceTag);
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
                emit(context, pk, TABLE_1);
                return;
            }

            if (TABLE_2.equalsIgnoreCase(table)) {
                // In T2(A3, A1), the third field stores A1.
                emit(context, attr, TABLE_2);
            }
        }
    }

    public static class SetDifferenceReducer extends Reducer<Text, Text, Text, NullWritable> {
        private boolean isOnlyInT1(Iterable<Text> values) {
            Set<String> sources = new HashSet<>();
            for (Text value : values) {
                sources.add(value.toString());
            }
            return sources.contains(TABLE_1) && !sources.contains(TABLE_2);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (isOnlyInT1(values)) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: SetDifference <in_path> <out_path>");
            System.err.println("Example: SetDifference input/ output/");
            System.err.println("Note: Output directory must not exist before running the job.");
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
