/**
 * Created by mi on 12/10/2017.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pivot {
    // Custom class for <LongWritable, Text> pair.
    public static class IdxPairWritable implements WritableComparable<IdxPairWritable> {
        private LongWritable idx1;
        private LongWritable idx2;

        // Default constructor
        public IdxPairWritable() {
            this.idx1 = new LongWritable();
            this.idx2 = new LongWritable();
        }

        // Custom constructor
        public IdxPairWritable(LongWritable idx1, LongWritable idx2) {
            this.idx1 = idx1;
            this.idx2 = idx2;
        }

        // Setter function to set the values of IntTextWritable object
        public void set(LongWritable idx1, LongWritable idx2) {
            this.idx1 = idx1;
            this.idx2 = idx2;
        }

        // Getter function for idx
        public LongWritable getIdx1() {
            return idx1;
        }

        // Getter function for content
        public LongWritable getIdx2() {
            return idx2;
        }

        @Override
        // Overriding default readFields function, it de-serializes the byte stresm data
        public void readFields(DataInput in) throws IOException {
            idx1.readFields(in);
            idx2.readFields(in);
        }

        @Override
        // Overriding default write function, it serializes object data into byte stream data
        public void write(DataOutput out) throws IOException {
            idx1.write(out);
            idx2.write(out);
        }

        @Override
        public int compareTo(IdxPairWritable o) {
            if (idx1.compareTo(o.idx1) != 0)
                return idx1.compareTo(o.idx1);
            else
                return idx2.compareTo(o.idx2);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof IdxPairWritable) {
                IdxPairWritable other = (IdxPairWritable) o;
                return idx1.equals(other.idx1) && idx2.equals(other.idx2);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return idx1.hashCode() & idx2.hashCode();
        }
    }

    // Custom partitioner function to partition base on idx1 only
    public static class IdxPairPartitioner extends Partitioner<IdxPairWritable, Text>{
        @Override
        public int getPartition(IdxPairWritable idxPair, Text text, int numPartitions) {
            return idxPair.getIdx1().hashCode() % numPartitions;
        }
    }

    // Custom group comparator to ensure the intermediate date are grouped by idx1
    public static class IdxPairGroupingComparator extends WritableComparator {
        public IdxPairGroupingComparator() {
            super(IdxPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            IdxPairWritable ip1 = (IdxPairWritable) o1;
            IdxPairWritable ip2 = (IdxPairWritable) o2;
            return ip1.getIdx1().compareTo(ip2.getIdx1());
        }
    }

    public static class PivotMapper
            extends Mapper<LongWritable, Text, IdxPairWritable, Text>{

        private IdxPairWritable keyPair = new IdxPairWritable();
        private LongWritable colIdx = new LongWritable();
        private Text content = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().trim().split(",");
            long i = 0;
            for (String f : fields) {
                colIdx.set(i);
                content.set(f);
                keyPair.set(colIdx, key);

                context.write(keyPair, content);
                i++;
            }
        }
    }

    public static class PivotReducer
            extends Reducer<IdxPairWritable, Text, LongWritable, Text> {

        private Text result = new Text();

        public void reduce(IdxPairWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String rowContent = "";
            for (Text field : values) {
                if (key.getIdx2().get() != 0)
                    rowContent += ",";
                rowContent += field;
            }
            result.set(rowContent);
            context.write(key.getIdx1(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pivot");
        job.setJarByClass(Pivot.class);
        job.setMapperClass(PivotMapper.class);
        //job.setCombinerClass(PivotReducer.class);
        job.setReducerClass(PivotReducer.class);
        job.setMapOutputKeyClass(IdxPairWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(IdxPairPartitioner.class);
        job.setGroupingComparatorClass(IdxPairGroupingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}