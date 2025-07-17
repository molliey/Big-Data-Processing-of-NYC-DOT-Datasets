import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class trafficFlowReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFlow = 0.0;
        int count = 0;

        for (Text value : values) {
            double flow = Double.parseDouble(value.toString());
            totalFlow += flow;
            count++;
        }

        double avgFlow = count == 0 ? 0 : totalFlow / count;

        result.set(String.format("%.5f", avgFlow));
        context.write(key, result);
    }
}