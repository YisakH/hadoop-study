package socks.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LogFileCombiner {
    public static class CombineMapper extends Mapper<Object, Text, Text, Text> {
        private final Text fileKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Key can be set to filename or any unique identifier
            fileKey.set("CombinedLogs");
            // Writing the value directly
            context.write(fileKey, value);
        }
    }

    public static class CombineReducer extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Simply write all values to the output file
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogFileCombiner");
        job.setJarByClass(LogFileCombiner.class);
        job.setMapperClass(CombineMapper.class);
        job.setReducerClass(CombineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}