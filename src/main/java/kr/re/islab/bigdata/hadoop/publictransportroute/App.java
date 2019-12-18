package kr.re.islab.bigdata.hadoop.publictransportroute;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import kr.re.islab.bigdata.hadoop.publictransportroute.mapper.ClusterCentererMapper;
import kr.re.islab.bigdata.hadoop.publictransportroute.mapper.ClusterMapper;
import kr.re.islab.bigdata.hadoop.publictransportroute.mapper.CoordinateMapper;
import kr.re.islab.bigdata.hadoop.publictransportroute.reducer.ClusterCenterReducer;
import kr.re.islab.bigdata.hadoop.publictransportroute.reducer.ClusterReducer;
import kr.re.islab.bigdata.hadoop.publictransportroute.reducer.HotSpotReducer;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.ClusterValueWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridCoordinateWriteable;
import kr.re.islab.bigdata.hadoop.publictransportroute.writeable.GridValueWriteable;

/**
 * By Naufal Suryanto
 *
 */
public class App {

    public static void hotspotGrid(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hotspot_grid");
        job.setJarByClass(App.class);
        job.setMapperClass(CoordinateMapper.class);
        job.setCombinerClass(HotSpotReducer.class);
        job.setReducerClass(HotSpotReducer.class);
        job.setOutputKeyClass(GridCoordinateWriteable.class);
        job.setOutputValueClass(GridValueWriteable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void clusterGrid(String[] args, boolean singleRun) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cluster_grid");
        job.setJarByClass(App.class);
        job.setMapperClass(ClusterMapper.class);
        job.setCombinerClass(ClusterReducer.class);
        job.setReducerClass(ClusterReducer.class);
        job.setOutputKeyClass(GridCoordinateWriteable.class);
        job.setOutputValueClass(ClusterValueWriteable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if(singleRun) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            job.waitForCompletion(true);
        }
        
    }

    public static void clusterCenterer(String[] args, boolean singleRun) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cluster_centerer");
        job.setJarByClass(App.class);
        job.setMapperClass(ClusterCentererMapper.class);
        job.setCombinerClass(ClusterCenterReducer.class);
        job.setReducerClass(ClusterCenterReducer.class);
        job.setOutputKeyClass(GridCoordinateWriteable.class);
        job.setOutputValueClass(GridValueWriteable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (singleRun) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            job.waitForCompletion(true);
        }
    }

    public static void clusterAuto(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
        int loopCount = Integer.parseInt(args[1]);
        int loopCount2 = Integer.parseInt(args[2]);
        String inputPath = args[3];
        String outputPath = args[4];

        String[] inputOutputPath = new String[3];
        inputOutputPath[0] = "cluster_auto";
        inputOutputPath[1] = inputPath;
        System.out.println("Starting Cluster Auto with : " + loopCount + " loop");
        for (int i = 0; i < loopCount; i++) {
            for(int j = 0; j < loopCount2; j++) {
                System.out.println("Running Cluster Grid at Loop " + (i + 1) + "," + (j + 1) );
                inputOutputPath[2] = String.valueOf(outputPath + "_grid_" + i + "_" + j);
                clusterGrid(inputOutputPath, false);
                inputOutputPath[1] = String.valueOf(inputOutputPath[2]);
               
            }
            inputOutputPath[2] = String.valueOf(outputPath + "_center_" + i);
            System.out.println("Running Cluster Centerer at Loop " + (i + 1));
            clusterCenterer(inputOutputPath, false);
            inputOutputPath[1] = String.valueOf(inputOutputPath[2]);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args[0].equals("hotspot_grid")) {
            hotspotGrid(args);
        } else if (args[0].equals("cluster_grid")) {
            clusterGrid(args, true);
        } else if (args[0].equals("cluster_centerer")) {
            clusterCenterer(args, true);
        } else if (args[0].equals("cluster_auto")) {    
            clusterAuto(args);
        }

    }
}
