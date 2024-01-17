package dke.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xbill.DNS.Tokenizer;

import java.io.IOException;
import java.util.StringTokenizer;


public class LogFileAnalysis {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class DeepLogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text logType = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Determine the type of log file from the file path or content
            // This is a simplified example. You might need a more robust way to determine the log type.
            if (line.contains("kernel:")) {
                // Process kernel log line
                String info = processKernelLog(line);
                logType.set(info);
            } else if (line.contains("UFW BLOCK")) {
                // Process UFW log line
                String info = processUfwLog(line);
                logType.set(info);
            } else if (line.contains("sshd")) {
                // Process auth log line
                String info = processAuthLog(line);
                logType.set(info);
            } else {
                // Process other syslog entries
                String info = processSysLog(line);
                logType.set(info);
            }

            context.write(logType, one);
        }

        private String processKernelLog(String line) {
            // Extract relevant information from kernel log
            return "KernelLogInfo";
        }

        private String processUfwLog(String line) {
            // Extract relevant information from UFW log
            return "UfwLogInfo";
        }

        private String processAuthLog(String line) {
            // Extract relevant information from auth log
            return "AuthLogInfo";
        }

        private String processSysLog(String line) {
            // Extract relevant information from general syslog
            return "SysLogInfo";
        }
    }

    public static class LogCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text logLevel = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String lowerCastString = line.toLowerCase();

            if(lowerCastString.contains("error")) {
                logLevel.set("ERROR");
                context.write(logLevel, one);
            } else if(lowerCastString.contains("info")) {
                logLevel.set("INFO");
                context.write(logLevel, one);
            } else if(lowerCastString.contains("debug")) {
                logLevel.set("DEBUG");
                context.write(logLevel, one);
            } else{
                logLevel.set("OTHER");
                context.write(logLevel, one);
            }
        }
    }
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log File Count Analysis");
        job.setJarByClass(LogFileAnalysis.class);
        job.setMapperClass(DeepLogMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}