import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CategoryMapping {
    public static class CategoryMapper extends Mapper<Object, Text, Text, Text> {
        private static final Map<String, Map<String, String>> CATEGORY_MAPPINGS = new HashMap<>();
        
        static {
            // SevereRentBurdenVsCity mapping
            Map<String, String> severeRentBurdenMap = new HashMap<>();
            severeRentBurdenMap.put("Larger proportion of severely rent burdened households than City", "2");
            severeRentBurdenMap.put("No statistical difference from the City in the proportion of severely rent burdened households", "1");
            severeRentBurdenMap.put("Smaller proportion of severely rent burdened households than City", "0");
            CATEGORY_MAPPINGS.put("SevereRentBurdenVsCity", severeRentBurdenMap);

            // NotRentStabilizedVsCity mapping
            Map<String, String> notRentStabilizedMap = new HashMap<>();
            notRentStabilizedMap.put("Larger proportion of households in rent-stabilized units than City", "2");
            notRentStabilizedMap.put("No statistical difference from the City in the proportion of rent-stabilized households", "1");
            notRentStabilizedMap.put("Smaller proportion of households in rent-stabilized units than City", "0");
            CATEGORY_MAPPINGS.put("NotRentStabilizedVsCity", notRentStabilizedMap);

            // 3PlusMaintenanceDeficienciesVsCity mapping
            Map<String, String> maintenanceDeficienciesMap = new HashMap<>();
            maintenanceDeficienciesMap.put("Larger proportion of units with 3+ maintenance deficiencies than the City", "2");
            maintenanceDeficienciesMap.put("No statistical difference from the City in the proportion of units with 3+ maintenance deficiencies", "1");
            maintenanceDeficienciesMap.put("Smaller proportion of units with 3+ maintenance deficiencies than the City", "0");
            CATEGORY_MAPPINGS.put("3PlusMaintenanceDeficienciesVsCity", maintenanceDeficienciesMap);

            // ChangeInRentsVsCity mapping
            Map<String, String> changeInRentsMap = new HashMap<>();
            changeInRentsMap.put("Larger rent change than City", "2");
            changeInRentsMap.put("No statistical difference in rent change from City", "1");
            changeInRentsMap.put("Smaller rent change than City", "0");
            CATEGORY_MAPPINGS.put("ChangeInRentsVsCity", changeInRentsMap);

            // ChangeInPopulationWithBachelorsDegreesVsCity mapping
            Map<String, String> bachelorsDegreeMap = new HashMap<>();
            bachelorsDegreeMap.put("Larger percent change of population with a bachelor's degree than the City", "2");
            bachelorsDegreeMap.put("No statistical difference from the City in the percent change of population with a bachelor's degree", "1");
            bachelorsDegreeMap.put("Smaller percent change of population with a bachelor's degree than the City", "0");
            CATEGORY_MAPPINGS.put("ChangeInPopulationWithBachelorsDegreesVsCity", bachelorsDegreeMap);

            // Adjacency mapping
            Map<String, String> adjacencyMap = new HashMap<>();
            adjacencyMap.put("Adjacent to neighborhoods with high Market Pressure scores", "1");
            adjacencyMap.put("Not adjacent to neighborhoods with high Market Pressure scores", "0");
            CATEGORY_MAPPINGS.put("Adjacency", adjacencyMap);
        }

        private String getMapping(String category, String value) {
            Map<String, String> categoryMap = CATEGORY_MAPPINGS.get(category);
            if (categoryMap == null) {
                return "-1";
            }
            return categoryMap.getOrDefault(value, "-1");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("NTAName")) {
                return;
            }

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length < 31) {
                return;
            }

            StringBuilder output = new StringBuilder();
            output.append(fields[0]).append("\t"); // NTAName

            String severeRentBurden = getMapping("SevereRentBurdenVsCity", fields[11]);
            String notRentStabilized = getMapping("NotRentStabilizedVsCity", fields[17]);
            String maintenanceDeficiencies = getMapping("3PlusMaintenanceDeficienciesVsCity", fields[20]);
            String changeInRents = getMapping("ChangeInRentsVsCity", fields[23]);
            String bachelorsDegree = getMapping("ChangeInPopulationWithBachelorsDegreesVsCity", fields[27]);
            String adjacency = getMapping("Adjacency", fields[30]);

            output.append(severeRentBurden).append("\t")
                  .append(notRentStabilized).append("\t")
                  .append(maintenanceDeficiencies).append("\t")
                  .append(changeInRents).append("\t")
                  .append(bachelorsDegree).append("\t")
                  .append(adjacency);

            context.write(new Text(fields[1]), new Text(output.toString()));
        }
    }

    public static class CategoryReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "category mapping");
        
        job.setJarByClass(CategoryMapping.class);
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}