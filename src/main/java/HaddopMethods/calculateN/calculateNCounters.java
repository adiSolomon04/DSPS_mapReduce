package HaddopMethods.calculateN;

import HaddopMethods.FirstWordMap.FirstWordMapOutput;
import HaddopMethods.SecondWordDecade.SecondWordDecadeMapData;
import MyDictionary.StopWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class calculateNCounters {
    public static class MapperClass extends Mapper<LongWritable,Text, calculateNMapData, IntWritable> {
        private StopWords stopWords;
        //private int maxPairs = 20;
        //private int iterations = 0;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            stopWords = new StopWords();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] KeyValue = value.toString().split("\t");
            String[] keys = KeyValue[0].toString().split(" ");
            String[] values = KeyValue[1].toString().split(" ");

            String w1 = keys[0];
            String w2 = keys[1];
            String Decade = keys[2];
            context.write(new calculateNMapData("*","*"), new IntWritable(Integer.parseInt(values[1])));
        }

        /*
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            while (context.nextKeyValue()) {
                if(iterations < maxPairs) {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                    iterations++;
                } else {
                    cleanup(context);
                    break;
                }
            }
        }*/

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    /*
    Reducer
     */
    public static class ReducerClass extends Reducer<calculateNMapData,IntWritable, calculateNMapData, IntWritable> {
        private int keypairSum;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(calculateNMapData key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for(IntWritable val : values)
                sum+= val.get();
            context.write(key, new IntWritable(sum));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    /*
    Combiner
     */
    public static class CombinerClass extends Reducer<calculateNMapData,IntWritable, calculateNMapData,IntWritable> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(calculateNMapData key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for(IntWritable val : values)
                sum+= val.get();
            context.write(key, new IntWritable(sum));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<calculateNMapData,IntWritable> {

        @Override
        public int getPartition(calculateNMapData key, IntWritable value, int numPartitions) {
            return key.w1.hashCode() % numPartitions; //only partition by w1
        }

    }

    public static Job getJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate N Counters");
        job.setJarByClass(calculateNCounters.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(calculateNMapData.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(calculateNMapData.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate N Counters");
        job.setJarByClass(calculateNCounters.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(calculateNMapData.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(calculateNMapData.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

