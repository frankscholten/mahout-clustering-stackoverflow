package bbuzz2011.stackoverflow.preprocess.text;

import bbuzz2011.stackoverflow.preprocess.xml.PostWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StackOverflowPostTextMapper extends Mapper<LongWritable, PostWritable, Text, Text> {

  private Text id = new Text();
  private Text text = new Text();

  @Override
  protected void map(LongWritable key, PostWritable value, Context context) throws IOException, InterruptedException {
    id.set(String.valueOf(key.get()));

    StringBuilder builder = new StringBuilder(value.getTitle()).append("\n\n").append(value.getContent());
    text.set(builder.toString());

    context.write(id, text);
  }
}
