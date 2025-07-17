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


public class DataCleaningMapper extends Mapper<Object, Text, Text, NullWritable> {
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
            context.write(new Text(line), NullWritable.get());
            return;
        }

        String[] fields = line.split(",");

        if (fields.length == 4) {
            String siteID = fields[1]; // second column
            String siteName = siteMap.getOrDefault(siteID, "Unknown Site");
            
            if (siteName.equals("Cross Bronx Expy") || siteName.equals("Van Wyck") || siteName.equals("Queens College")) {
                return; // Skip incomplete records based on data profiling result
            }
            // Reconstruct the line with SiteName
            String replacedLine = String.format("%s,%s,%s,%s", fields[0], siteName, fields[2], fields[3]);

            context.write(new Text(replacedLine), NullWritable.get());
        }
    }
}