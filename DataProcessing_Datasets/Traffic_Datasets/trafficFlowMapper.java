import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

public class trafficFlowMapper extends Mapper<Object, Text, Text, Text> {
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:00");

    private static final Map<String, String> SITE_INFO = new HashMap<>();

    static {
        SITE_INFO.put("36061NY12380", "CBE E AMSTERDAM AVE(U/LVL) - MORRIS AVE");
        SITE_INFO.put("36085NY04805", "SIE E-MLK N RICHMOND AVENUE - WALKER STREET");
        SITE_INFO.put("36061NY09929", "LINCOLN TUNNEL E CENTER TUBE NJ - NY");
        SITE_INFO.put("36061NY10130", "FDR S 63rd - 25th St");
        SITE_INFO.put("36061NY08454", "FDR S Catherine Slip - BKN Bridge Manhattan Side");
        SITE_INFO.put("36085NY03820", "SIE W - MLK N WOOLEY AVENUE - WLAKER STREET");
        SITE_INFO.put("36005NY11534", "TBB W - FDR S MANHATTAN TRUSS - E116TH STREET");
        SITE_INFO.put("36061NY08552", "FDR N Catherine Slip - 25th St");
        SITE_INFO.put("36061NY09734", "LINCOLN TUNNEL W CENTER TUBE NY - NJ");
        SITE_INFO.put("36061NY08653", "FDR N Catherine Slip - 25th St");
        SITE_INFO.put("36005NY11790", "BE N STRATFORD AVENUE - CASTLE HILL AVE");
        SITE_INFO.put("36081NY07615", "VWE S MP4.63 (Exit 6 Jamaica Ave) - MP2.66 (Exit 2 Roackaway Blvd)");
        SITE_INFO.put("36081NY09285", "VWE N MP4.63 (Exit 6 - Jamaica Ave) - MP6.39 (Exit 11 Jewel Ave)");
        SITE_INFO.put("36005NY12387", "MDE S HARLEM RIVER PARK - GWB W AMSTERDAM AVENUE LOWER LEVEL");
        SITE_INFO.put("36047NY08274", "FDR N Catherine Slip - 25th St");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("SPEED")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 5) {
            return; 
        }

        try {
            String timestamp = fields[2];  // DATA_AS_OF
            String borough = fields[3];  // BOROUGH
            String linkName = fields[4]; // LINK_NAME
            double speed = Double.parseDouble(fields[0]);    // SPEED
            double travelTime = Double.parseDouble(fields[1]); // TRAVEL_TIME
            
            String siteID = SITE_INFO.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(linkName))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);

            if (siteID != null) {
                String dateHour = outputFormat.format(inputFormat.parse(timestamp));
                double trafficFlow = travelTime > 0 ? speed / travelTime : 0;

                outputKey.set(dateHour + "\t" + siteID + "\t" + borough + "\t" + linkName);
                outputValue.set(String.valueOf(trafficFlow));

                context.write(outputKey, outputValue);
            }
        } catch (ParseException | NumberFormatException e) {
            System.err.println("Error parsing line: " + line);
        }
    }
}