package bbuzz2011.stackoverflow.preprocess.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

import java.io.IOException;

public class StackOverflowPostXMLParserJob extends Configured {

  public static final String INPUT = StackOverflowPostXMLParserJob.class.getSimpleName() + "-input";
  public static final String OUTPUT = StackOverflowPostXMLParserJob.class.getSimpleName() + "-output";

  public static final String OUTPUT_POSTS_PATH = "posts/";

  private Path postOutputPath;

  public StackOverflowPostXMLParserJob(Configuration configuration) {
    super(configuration);
  }

  public Path parseXML() throws ClassNotFoundException, IOException, InterruptedException {
    Configuration configuration = getConf();

    configuration.set(XmlInputFormat.START_TAG_KEY, "<row Id=");
    configuration.set(XmlInputFormat.END_TAG_KEY, " />");

    Job job = new Job(configuration);
    job.setInputFormatClass(XmlInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(PostWritable.class);

    Path inputPath = new Path(configuration.get(INPUT));
    Path outputBasePath = new Path(configuration.get(OUTPUT));
    postOutputPath = new Path(outputBasePath, OUTPUT_POSTS_PATH);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, postOutputPath);

    job.setJarByClass(StackOverflowPostXMLParserJob.class);
    job.setMapperClass(StackOverflowPostXMLMapper.class);
    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("StackOverflow post XML parser failed processing");
    }

    return postOutputPath;
  }

  public Path getPostOutputPath() {
    return postOutputPath;
  }
}
