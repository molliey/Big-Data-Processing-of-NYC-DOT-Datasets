import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class DataProfilingMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Map<String, String> siteMap = new HashMap<>();

    @Override
    protected void setup(Context context) {
        siteMap.put("36061NY08454", "Manhattan Bridge");
        siteMap.put("36005NY11790", "Hunts Point");
        siteMap.put("36081NY08198", "Glendale");
        siteMap.put("36081NY09285", "Queens College");
        siteMap.put("36061NY09734", "Broadway/35th St");
        siteMap.put("36061NY08653", "FDR");
        siteMap.put("36005NY11534", "Mott Haven");
        siteMap.put("36005NY12387", "Cross Bronx Expy");
        siteMap.put("36081NY07615", "Van Wyck");
        siteMap.put("36047NY07974", "BQE");
        siteMap.put("36047NY08274", "Williamsburg");
        siteMap.put("36061NY10130", "Queensboro Bridge");
        siteMap.put("36061NY08552", "Williamsburg Bridge");
        siteMap.put("36061NY12380", "Hamilton Bridge");
        siteMap.put("36061NY09929", "Midtown-DOT");
        siteMap.put("36085NY04805", "Port Richmond");
        siteMap.put("36085NY03820", "SI Expwy");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("ID")) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length == 4) {
            String siteID = fields[1]; // second column
            String valueStr = fields[3]; // fourth column

            String siteName = siteMap.getOrDefault(siteID, "Unknown Site");

            try {
                Double observationValue = Double.parseDouble(valueStr);

                word.set(siteName);
                context.write(word, one); // Count by SiteName
            } catch (NumberFormatException e) {
                word.set("Invalid Value");
                context.write(word, one); // Count invalid values
            }
        } else {
            word.set("Invalid Record");
            context.write(word, one); // Count invalid records
        }
    }
}