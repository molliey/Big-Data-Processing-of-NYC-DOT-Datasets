import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataCleaning {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Data Cleaning <input path> <output path>");
            System.exit(-1);
        }
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "replace site id with site name");
        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0); // No reducer needed
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}