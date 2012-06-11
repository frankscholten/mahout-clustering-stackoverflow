package bbuzz2011.stackoverflow.preprocess.xml;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Contains title and content of a StackOverflow post.
 */
public class PostWritable implements Writable {

  private String title;
  private String content;

  public PostWritable(String title, String content) {
    this.title = title;
    this.content = content;
  }

  public PostWritable() {
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(title);
    out.writeUTF(content);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.title = in.readUTF();
    this.content = in.readUTF();
  }

  public String getTitle() {
    return title;
  }

  public String getContent() {
    return content;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setContent(String content) {
    this.content = content;
  }

  @Override
  public String toString() {
    return "title = " + title + ", content = " + content;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PostWritable that = (PostWritable) o;

    if (content != null ? !content.equals(that.content) : that.content != null) return false;
    if (title != null ? !title.equals(that.title) : that.title != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = title != null ? title.hashCode() : 0;
    result = 31 * result + (content != null ? content.hashCode() : 0);
    return result;
  }
}
