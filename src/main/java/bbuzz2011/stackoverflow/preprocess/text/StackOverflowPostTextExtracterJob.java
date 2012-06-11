package bbuzz2011.stackoverflow.preprocess.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class StackOverflowPostTextExtracterJob extends Configured {

  public static final String INPUT = StackOverflowPostTextExtracterJob.class.getSimpleName() + "-input";
  public static final String OUTPUT = StackOverflowPostTextExtracterJob.class.getSimpleName() + "-output";
  public static final String OUTPUT_POSTS_TEXT = "posts-text/";

  public StackOverflowPostTextExtracterJob(Configuration configuration) {
    super(configuration);
  }

  public Path run() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path inputPath = new Path(configuration.get(INPUT));
    Path outputBasePath = new Path(configuration.get(OUTPUT));
    Path textOutputPath = new Path(outputBasePath, OUTPUT_POSTS_TEXT);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, textOutputPath);

    job.setJarByClass(StackOverflowPostTextMapper.class);
    job.setMapperClass(StackOverflowPostTextMapper.class);
    job.setNumReduceTasks(0);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("StackOverflow post XML parser failed processing");
    }

    return textOutputPath;
  }
}
