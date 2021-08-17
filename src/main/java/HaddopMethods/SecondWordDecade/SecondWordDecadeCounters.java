package HaddopMethods.SecondWordDecade;

import HaddopMethods.FirstWordMap.FirstWordMapData;
import HaddopMethods.FirstWordMap.FirstWordMapOutput;
import MyDictionary.StopWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SecondWordDecadeCounters {

    public static class MapperClass extends Mapper<LongWritable,Text, SecondWordDecadeMapData, FirstWordMapOutput> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] KeyValue = value.toString().split("\t");
            String[] keys = KeyValue[0].toString().split(" ");
            String[] values = KeyValue[1].toString().split(" ");
            FirstWordMapOutput parsedValue = new FirstWordMapOutput(Integer.parseInt(values[0]), Integer.parseInt(values[1]));

            String w1 = keys[0];
            String w2 = keys[1];
            String Decade = keys[2];
            //todo: save in the last map reduce <*,w1> and add here if, in the combinner and reducer count count_w1
            context.write(new SecondWordDecadeMapData(w1, w2, Decade), parsedValue);
            context.write(new SecondWordDecadeMapData("*",w2,"*"), parsedValue);
        }
        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    /*
    Reducer
     */
    public static class ReducerClass extends Reducer<SecondWordDecadeMapData, FirstWordMapOutput, SecondWordDecadeMapData, DoubleWritable> {
        private int keypairSum_decade;
        private int keypairSum_w2;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            keypairSum_w2 = 0;
            keypairSum_decade = Integer.parseInt(context.getConfiguration().get("N"));
        }

        @Override
        public void reduce(SecondWordDecadeMapData key, Iterable<FirstWordMapOutput> values, Context context) throws IOException,  InterruptedException {
            if(key.w1.equals("*")){
                int sum = 0;
                for (FirstWordMapOutput val : values)
                    sum += val.count_w1w2;
                keypairSum_w2 = sum;
            }
            else {
                FirstWordMapOutput next = values.iterator().next();
                double pmi = Math.log10(next.count_w1w2)+Math.log10(keypairSum_decade)-Math.log10(next.count_w1)-Math.log10(keypairSum_w2);
                double npmi = pmi/(-Math.log10(((double)(next.count_w1w2))/keypairSum_decade));
                context.write(key, new DoubleWritable(npmi));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    /*
    Combiner
     */
    public static class CombinerClass extends Reducer<SecondWordDecadeMapData, FirstWordMapOutput, SecondWordDecadeMapData, FirstWordMapOutput> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(SecondWordDecadeMapData key, Iterable<FirstWordMapOutput> values, Context context) throws IOException,  InterruptedException {
            if(key.w1.equals("*")) {
                int sum = 0;
                for (FirstWordMapOutput val : values)
                    sum += val.count_w1w2;
                context.write(key, new FirstWordMapOutput(0,sum));//dont-care
            }
            else
                context.write(key, values.iterator().next());
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<SecondWordDecadeMapData,FirstWordMapOutput> {
        @Override
        public int getPartition(SecondWordDecadeMapData key, FirstWordMapOutput value, int numPartitions) {
            return key.w2.hashCode() % numPartitions; //only partition by Decade
        }

    }

    public static Job getJob(String N) throws IOException {
        Configuration conf = new Configuration();
        conf.set("N", N);
        Job job = Job.getInstance(conf, "Second Word Decade Counters");
        job.setJarByClass(SecondWordDecadeCounters.class);
        job.setMapperClass(SecondWordDecadeCounters.MapperClass.class);
        job.setPartitionerClass(SecondWordDecadeCounters.PartitionerClass.class);
        //job.setCombinerClass(SecondWordDecadeCounters.CombinerClass.class);
        job.setReducerClass(SecondWordDecadeCounters.ReducerClass.class);
        job.setMapOutputKeyClass(SecondWordDecadeMapData.class);
        job.setMapOutputValueClass(FirstWordMapOutput.class);
        job.setOutputKeyClass(SecondWordDecadeMapData.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Second Word Decade Counters");
        job.setJarByClass(SecondWordDecadeCounters.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(SecondWordDecadeMapData.class);
        job.setMapOutputValueClass(FirstWordMapOutput.class);
        job.setOutputKeyClass(SecondWordDecadeMapData.class);
        job.setOutputValueClass(DoubleWritable.class);
        //todo: set input file format https://intellitech.pro/tutorial-4-hadoop-custom-input-format/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

