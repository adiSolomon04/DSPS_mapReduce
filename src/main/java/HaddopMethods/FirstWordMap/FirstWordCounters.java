package HaddopMethods.FirstWordMap;
import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FirstWordCounters {
    public static class MapperClass extends Mapper<LongWritable,Text,FirstWordMapData, IntWritable> {
        private StopWords stopWords;
        //private int maxPairs = 20;
        //private int iterations = 0;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            stopWords = new StopWords();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] gram = value.toString().split("\\s+"); //length
            try {
                if (!stopWords.isStop(gram[0]) & !stopWords.isStop(gram[1])) {
                    String Decade = gram[2].substring(0, gram[2].length() - 1);
                    context.write(new FirstWordMapData(gram[0], gram[1], Decade),
                            new IntWritable(Integer.parseInt(gram[3])));
                    context.write(new FirstWordMapData(gram[0], "*", "*"),
                            new IntWritable(Integer.parseInt(gram[3])));
                }
            }catch (Exception e){
                System.out.println("\n\n\nErr Adi print "+key+" "+value+"\n\n\n");
            }
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
    public static class ReducerClass extends Reducer<FirstWordMapData,IntWritable,FirstWordMapData,FirstWordMapOutput> {
        private int keypairSum;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            keypairSum = 0;
        }

        @Override
        public void reduce(FirstWordMapData key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for(IntWritable val : values)
                sum+= val.get();
            if(key.w2.equals("*")) {
                keypairSum = sum;
            }
            else
                context.write(key, new FirstWordMapOutput(keypairSum, sum));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    /*
    Combiner
     */
    public static class CombinerClass extends Reducer<FirstWordMapData,IntWritable,FirstWordMapData,IntWritable> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(FirstWordMapData key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for(IntWritable val : values)
                sum+= val.get();
            context.write(key, new IntWritable(sum));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<FirstWordMapData,IntWritable> {

        @Override
        public int getPartition(FirstWordMapData key, IntWritable value, int numPartitions) {
            return key.w1.hashCode() % numPartitions; //only partition by w1
        }

    }

    public static Job getJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "First Word Counters");
        job.setJarByClass(FirstWordCounters.class);
        job.setMapperClass(FirstWordCounters.MapperClass.class);
        job.setPartitionerClass(FirstWordCounters.PartitionerClass.class);
        //job.setCombinerClass(FirstWordCounters.CombinerClass.class);
        job.setReducerClass(FirstWordCounters.ReducerClass.class);
        job.setMapOutputKeyClass(FirstWordMapData.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FirstWordMapData.class);
        job.setOutputValueClass(FirstWordMapOutput.class);
        job.setInputFormatClass(TextInputFormat.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "First Word Counters");
        job.setJarByClass(FirstWordCounters.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(FirstWordMapData.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FirstWordMapData.class);
        job.setOutputValueClass(FirstWordMapOutput.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

