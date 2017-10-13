/**
 * Created by mi on 12/10/2017.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import com.sun.xml.txw2.output.IndentingXMLFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pivot {
    // Custom class for <LongWritable, Text> pair.
    public static class IdxTextPairWritable implements WritableComparable<IdxTextPairWritable> {
        private LongWritable idx;
        private Text content;

        // Default constructor
        public IdxTextPairWritable() {
            this.idx = new LongWritable();
            this.content = new Text();
        }

        // Custom constructor
        public IdxTextPairWritable(LongWritable idx, Text content) {
            this.idx = idx;
            this.content = content;
        }

        // Setter function to set the values of IntTextWritable object
        public void set(LongWritable idx, Text content) {
            this.idx = idx;
            this.content = content;
        }

        // Getter function for idx
        public LongWritable getIdx() {
            return idx;
        }

        // Getter function for content
        public Text getContent() {
            return content;
        }

        @Override
        // Overriding default readFields function, it de-serializes the byte stresm data
        public void readFields(DataInput in) throws IOException {
            idx.readFields(in);
            content.readFields(in);
        }

        @Override
        // Overriding default write function, it serializes object data into byte stream data
        public void write(DataOutput out) throws IOException {
            idx.write(out);
            content.write(out);
        }

        @Override
        public int compareTo(IdxTextPairWritable o) {
            return idx.compareTo(o.idx);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof IdxTextPairWritable) {
                IdxTextPairWritable other = (IdxTextPairWritable) o;
                return idx.equals(other.idx);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return idx.hashCode();
        }
    }

    public static class PivotMapper
            extends Mapper<LongWritable, Text, LongWritable, IdxTextPairWritable>{

        private IdxTextPairWritable field = new IdxTextPairWritable();
        private LongWritable colIdx = new LongWritable();
        private Text content = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            long i = 0;
            for (String f : fields) {
                colIdx.set(i);
                content.set(f);
                field.set(key, content);

                context.write(colIdx, field);
                i++;
            }
        }
    }

    public static class PivotReducer
            extends Reducer<LongWritable, IdxTextPairWritable, LongWritable, Text> {

        private Text result = new Text();

        public void reduce(LongWritable key, Iterable<IdxTextPairWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String rowContent = "";

            for (IdxTextPairWritable field : values) {
                rowContent = field.getContent() + rowContent;
                if (field.getIdx().get() != 0)
                    rowContent = "," + rowContent;
            }

            result.set(rowContent);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pivot");
        job.setJarByClass(Pivot.class);
        job.setMapperClass(PivotMapper.class);
        //job.setCombinerClass(PivotReducer.class);
        job.setReducerClass(PivotReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IdxTextPairWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}