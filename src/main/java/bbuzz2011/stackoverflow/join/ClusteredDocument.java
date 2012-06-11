package bbuzz2011.stackoverflow.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusteredDocument implements Writable {

  private IntWritable clusterId = new IntWritable();
  private Text documentTitle = new Text();
  private Text documentContent = new Text();
  private List<String> topTerms = new ArrayList<String>();

  public ClusteredDocument() {
  }

  public ClusteredDocument(int clusterId, String documentTitle, String documentContent) {
    this.clusterId = new IntWritable(clusterId);
    this.documentTitle = new Text(documentTitle);
    this.documentContent = new Text(documentContent);
  }

  public void setClusterId(int clusterId) {
    this.clusterId.set(clusterId);
  }

  public void setDocumentTitle(String documentTitle) {
    this.documentTitle.set(documentTitle);
  }

  public void setDocumentContent(String documentContent) {
    this.documentContent.set(documentContent);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(clusterId.get());
    out.writeUTF(documentTitle.toString());
    out.writeUTF(documentContent.toString());
    out.writeInt(topTerms.size());
    for (String topTerm : topTerms) {
      out.writeUTF(topTerm);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clusterId = new IntWritable(in.readInt());
    documentTitle = new Text(in.readUTF());
    documentContent = new Text(in.readUTF());
    int topTermsSize = in.readInt();
    topTerms = new ArrayList<String>();
    for (int i = 0; i < topTermsSize; i++) {
      topTerms.add(in.readUTF());
    }
  }

  public Text getDocumentContent() {
    return documentContent;
  }

  public Text getDocumentTitle() {
    return documentTitle;
  }

  public IntWritable getClusterId() {
    return clusterId;
  }

  public List<String> getTopTerms() {
    return topTerms;
  }

  @Override
  public String toString() {
    return clusterId + "\t" + documentTitle + "\t" + documentContent + "\t" + topTerms;
  }

  public void setTopTerms(List<String> topTerms) {
    this.topTerms = topTerms;
  }
}
