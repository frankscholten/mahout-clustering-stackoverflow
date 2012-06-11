package bbuzz2011.stackoverflow.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class PointToClusterMappingJob extends Configured {

  private Path clusteredPointsPath;
  private Path pointsToClusterPath;

  public PointToClusterMappingJob(Path clusteredPointsPath, Path pointsToClusterPath) {
    this.clusteredPointsPath = clusteredPointsPath;
    this.pointsToClusterPath = pointsToClusterPath;
  }

  public void mapPointsToClusters() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration, PointToClusterMappingJob.class.getSimpleName());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(PointToClusterMappingJob.class);

    SequenceFileInputFormat.addInputPath(job, clusteredPointsPath);
    SequenceFileOutputFormat.setOutputPath(job, pointsToClusterPath);

    job.setMapperClass(PointToClusterMapper.class);

    job.setNumReduceTasks(1);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.waitForCompletion(true);
  }
}
