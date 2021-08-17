import HaddopMethods.FirstWordMap.FirstWordCounters;
import HaddopMethods.SecondWordDecade.SecondWordDecadeCounters;
import HaddopMethods.ThirdRelativeNMPISort.ThirdRelativeNMPISortCounters;
import HaddopMethods.calculateN.calculateNCounters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.LinkedList;

public class CollocationExtraction {
    //input nmpi mpi inputpath

    public static void main(String[] args) {
        try {
            Job job1 = FirstWordCounters.getJob();
            FileInputFormat.addInputPath(job1, new Path(args[2]));
            //todo: add 2 gram to input path
            FileOutputFormat.setOutputPath(job1, new Path("/firstMapOutput"));

            Job jobN = calculateNCounters.getJob();
            FileInputFormat.addInputPath(jobN, new Path("/firstMapOutput"));
            FileOutputFormat.setOutputPath(jobN, new Path("/outN"));

            job1.waitForCompletion(true);
            jobN.waitForCompletion(true);

            // -------read N ---------
            String filename = "/outN/part-r-00000";
            Path file = new Path(filename);
            //Path file2 = new Path("/lalaalal");
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            //hdfs.create(file2);
            InputStream Readfile = hdfs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(Readfile));
            String st;
            st = br.readLine();
            String [] splits = st.split("\t");
            String N = splits[1];
            // ----------------------

            Job job2 = SecondWordDecadeCounters.getJob(N);
            FileInputFormat.addInputPath(job2, new Path("/firstMapOutput"));
            FileOutputFormat.setOutputPath(job2, new Path("/secondMapOutput"));

            Job job3 = ThirdRelativeNMPISortCounters.getJob(args[0], args[1]);
            FileInputFormat.addInputPath(job3, new Path("/secondMapOutput"));
            FileOutputFormat.setOutputPath(job3, new Path("/MapReduceOUTPUT"));


            //todo: remove the middle folders.
            job2.waitForCompletion(true);
            job3.waitForCompletion(true);
            /*
            String command = "rm -r firstMapOutput secondMapOutput";
            Process process = Runtime.getRuntime().exec(command);
             */
        } catch (InterruptedException|IOException|ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error caught");
        }
        /*
        ControlledJob controlledJob1 = new ControlledJob(job1,new LinkedList<ControlledJob>());
        ControlledJob controlledJob2 = new ControlledJob(job2,new LinkedList<ControlledJob>());
        ControlledJob controlledJob3 = new ControlledJob(job3,new LinkedList<ControlledJob>());
        //controlledJob2.addDependingJob(controlledJob1);
        //controlledJob3.addDependingJob(controlledJob2);
        JobControl jc = new JobControl("JC");
        jc.addJob(controlledJob1);
        //jc.addJob(controlledJob2);
        //jc.addJob(controlledJob3);
        jc.run();
         */
    }
}
