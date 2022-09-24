import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

public class SortWordCountJob {
    public static class WordCountObj implements WritableComparable<WordCountObj> {
        private String word;
        private Long count;

        public WordCountObj() {
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public int compareTo(WordCountObj o) {
            int countCompare = count.compareTo(o.getCount());
            return countCompare == 0 ? word.compareTo(o.getWord()) : countCompare;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(word);
            dataOutput.writeLong(count);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            word = dataInput.readUTF();
            count = dataInput.readLong();
        }

        @Override
        public int hashCode() {
            int hashCode = 7;
            int result = 1;
            result = hashCode * result + (word == null ? 0 : word.hashCode());
            result = hashCode * result + (count == null ? 0 : count.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WordCountObj that = (WordCountObj) o;
            return Objects.equals(word, that.word) && Objects.equals(count, that.count);
        }

        @Override
        public String toString() {
            return word + " " + count;
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, WordCountObj, NullWritable> {

        private WordCountObj wordCountObj = new WordCountObj();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");

            while (itr.hasMoreTokens()) {
                var inputStr = itr.nextToken().split("\\s+");
                wordCountObj.setWord(inputStr[0]);
                wordCountObj.setCount(Long.valueOf(inputStr[1]));
                context.write(wordCountObj, NullWritable.get());
            }
        }
    }

    public static class FormatReducer extends Reducer<WordCountObj,NullWritable,Text,LongWritable> {
        @Override
        public void reduce(WordCountObj key, Iterable<NullWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getWord()), new LongWritable(key.getCount()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count sort");
        job.setJarByClass(SortWordCountJob.class);
        job.setMapperClass(SortWordCountJob.TokenizerMapper.class);
        job.setReducerClass(SortWordCountJob.FormatReducer.class);
        job.setMapOutputKeyClass(WordCountObj.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
