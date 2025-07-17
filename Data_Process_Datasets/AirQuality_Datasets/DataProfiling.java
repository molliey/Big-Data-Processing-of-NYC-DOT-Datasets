import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataProfiling {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Data Profilling <input path> <output path>");
            System.exit(-1);
        }
    
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "data profilling");
        job.setJarByClass(DataProfiling.class);
        job.setMapperClass(DataProfilingMapper.class);
        job.setCombinerClass(DataProfilingReducer.class);
        job.setReducerClass(DataProfilingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}