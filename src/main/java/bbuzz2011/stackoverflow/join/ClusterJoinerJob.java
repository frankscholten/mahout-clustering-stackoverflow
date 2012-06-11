package bbuzz2011.stackoverflow.join;

import bbuzz2011.stackoverflow.preprocess.xml.PostWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

import java.io.IOException;

public class ClusterJoinerJob extends Configured {

  private Path clusteredPointsPath;
  private Path documentPath;
  private Path outputPath;

  public ClusterJoinerJob(Path postPath, Path clusteredPointsPath, Path outputPath) {
    this.clusteredPointsPath = clusteredPointsPath;
    this.documentPath = postPath;
    this.outputPath = outputPath;
  }

  public void run() throws Exception {
    Configuration configuration = getConf();

    JobConf job = new JobConf(configuration);
    job.setInputFormat(CompositeInputFormat.class);
    job.set("mapred.join.expr", CompositeInputFormat.compose(
        "inner", SequenceFileInputFormat.class, documentPath, clusteredPointsPath));
    job.setOutputFormat(SequenceFileOutputFormat.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    job.setMapperClass(ClusterJoinMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(ClusteredDocument.class);
    job.setJarByClass(ClusterJoinerJob.class);

    job.setJobName(ClusterJoinerJob.class.getSimpleName());

    JobClient.runJob(new JobConf(job));
  }

  private static class ClusterJoinMapper extends MapReduceBase implements Mapper<LongWritable, TupleWritable, LongWritable, ClusteredDocument> {
    private ClusteredDocument clusteredDocument = new ClusteredDocument();

    @Override
    public void map(LongWritable key, TupleWritable value, OutputCollector<LongWritable, ClusteredDocument> output, Reporter reporter) throws IOException {
      int clusterId = ((IntWritable) value.get(1)).get();
      clusteredDocument.setClusterId(clusterId);

      PostWritable postWritable = (PostWritable) value.get(0);

      clusteredDocument.setDocumentTitle(postWritable.getTitle());
      clusteredDocument.setDocumentContent(postWritable.getContent());

      output.collect(key, clusteredDocument);
    }
  }
}