import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WeatherJob {

    public static class WeatherMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private static final int DATE_INDEX = 0;
        private static final int TEMPERATURE_INDEX = 1;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            var tokenizer = new StringTokenizer(value.toString(), "\r\n");
            while (tokenizer.hasMoreTokens()) {
                var data = tokenizer.nextToken().split(";");
                var monthYear = extractDateMonthYear(data);
                var temperature = extractTemperature(data);

                if (temperature != null) {
                    context.write(new Text(monthYear), new DoubleWritable(temperature));
                }
            }
        }

        private Double extractTemperature(String[] data) {
            String temperatureStr = data[TEMPERATURE_INDEX].replace("\"", "");
            try {
                return Double.parseDouble(temperatureStr);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private String extractDateMonthYear(String[] data) {
            String[] dateTimeSplit = data[DATE_INDEX].split("\\s+");
            String date = dateTimeSplit[0].replace("\"", "");
            return date.substring(date.indexOf('.') + 1);
        }
    }

    public static class WeatherMonthYearAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                              Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (var value : values) {
                sum += value.get();
                count++;
            }
            context.write(key, new DoubleWritable(count != 0 ? sum / count : sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather");
        job.setJarByClass(WeatherJob.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherMonthYearAverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}