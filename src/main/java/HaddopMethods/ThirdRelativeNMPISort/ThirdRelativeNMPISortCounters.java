package HaddopMethods.ThirdRelativeNMPISort;

import HaddopMethods.FirstWordMap.FirstWordMapOutput;
import HaddopMethods.SecondWordDecade.SecondWordDecadeMapData;
import MyDictionary.StopWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ThirdRelativeNMPISortCounters {
    private static String contextminNMPI = "minNMPI";
    private static String contextminRelativeNMPI = "minRelativeNMPI";

    public static class MapperClass extends Mapper<LongWritable,Text, ThirdRelativeNMPISortMapDataOrder, DoubleWritable> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] KeyValue = value.toString().split("\t");
            String[] keys = KeyValue[0].split(" ");
            double valueNMPI = Double.parseDouble(KeyValue[1]);
            //FirstWordMapOutput parsedValue = new FirstWordMapOutput(Integer.parseInt(valueNMPI), Integer.parseInt(valueNMPI));

            String w1 = keys[0];
            String w2 = keys[1];
            String Decade = keys[2];
            context.write(new ThirdRelativeNMPISortMapDataOrder(w1, w2, Decade, valueNMPI), new DoubleWritable(valueNMPI));
            context.write(new ThirdRelativeNMPISortMapDataOrder("*","*",Decade, valueNMPI), new DoubleWritable(valueNMPI));
        }
        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    /*
    Reducer
     */
    public static class ReducerClass extends Reducer<ThirdRelativeNMPISortMapDataOrder, DoubleWritable, ThirdRelativeNMPISortMapDataOrder, DoubleWritable> {
        private double nmpiSum_decade;
        private double minNMPI;
        private double minRelativeNMPI;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            nmpiSum_decade = 0;
            minNMPI = Double.parseDouble(context.getConfiguration().get(contextminNMPI));
            minRelativeNMPI = Double.parseDouble(context.getConfiguration().get(contextminRelativeNMPI));
        }

        @Override
        public void reduce(ThirdRelativeNMPISortMapDataOrder key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            if(key.w1.equals("*")&key.w2.equals("*")) { //sum all decade appearances
                double sum = 0;
                for (DoubleWritable val : values)
                    sum += val.get();
                nmpiSum_decade = sum;
            }
            else {                                      //sum only a single appearance of w1 w2 and calc relative
                double nmpi = values.iterator().next().get();
                double relativeNMPI = nmpi/nmpiSum_decade;
                if(nmpi>minNMPI||relativeNMPI>minRelativeNMPI)
                    context.write(new ThirdRelativeNMPISortMapDataOrder(key.w1, key.w2, key.Decade, nmpi), new DoubleWritable(nmpi));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    /*
    Combiner
     */
    public static class CombinerClass extends Reducer<ThirdRelativeNMPISortMapDataOrder, DoubleWritable, ThirdRelativeNMPISortMapDataOrder, DoubleWritable> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(ThirdRelativeNMPISortMapDataOrder key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            if(key.w1.equals("*")&key.w2.equals("*")) {
                double sum = 0;
                for (DoubleWritable val : values)
                    sum += val.get();
                context.write(key, new DoubleWritable(sum));//dont-care
            }
            else
                context.write(key, values.iterator().next());
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<ThirdRelativeNMPISortMapDataOrder, DoubleWritable> {
        @Override
        public int getPartition(ThirdRelativeNMPISortMapDataOrder key, DoubleWritable value, int numPartitions) {
            return key.Decade.hashCode() % numPartitions; //only partition by Decade
        }

    }

    public static Job getJob(String minNMPI, String minRelativeNPMI) throws IOException {
        Configuration conf = new Configuration();
        conf.set(contextminNMPI, minNMPI);
        conf.set(contextminRelativeNMPI, minRelativeNPMI);
        //todo: add filter to map
        Job job = Job.getInstance(conf, "Third RelativeNMPI Sort Counters");
        job.setJarByClass(ThirdRelativeNMPISortCounters.class);
        job.setMapperClass(ThirdRelativeNMPISortCounters.MapperClass.class);
        job.setPartitionerClass(ThirdRelativeNMPISortCounters.PartitionerClass.class);
        //job.setCombinerClass(ThirdRelativeNMPISortCounters.CombinerClass.class);
        job.setReducerClass(ThirdRelativeNMPISortCounters.ReducerClass.class);
        job.setMapOutputKeyClass(ThirdRelativeNMPISortMapDataOrder.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(ThirdRelativeNMPISortMapDataOrder.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);
        return job;
    }
    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(ThirdRelativeNMPISortMapDataOrder.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            ThirdRelativeNMPISortMapDataOrder key1 = (ThirdRelativeNMPISortMapDataOrder) w1;
            ThirdRelativeNMPISortMapDataOrder key2 = (ThirdRelativeNMPISortMapDataOrder) w2;
            return key1.compareTo2(key2);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Third RelativeNMPI Sort Counters");
        job.setJarByClass(ThirdRelativeNMPISortCounters.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(SecondWordDecadeMapData.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(ThirdRelativeNMPISortMapDataOrder.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

