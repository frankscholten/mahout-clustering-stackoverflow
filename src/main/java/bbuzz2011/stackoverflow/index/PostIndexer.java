package bbuzz2011.stackoverflow.index;

import bbuzz2011.stackoverflow.join.ClusteredDocument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.vectors.VectorHelper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

public class PostIndexer extends Configured {

  private Path clusteredPostsPath;
  private Path clusterPath;
  private String outputDictionaryPattern;

  private Map<Integer, String> clusterNames = new HashMap<Integer, String>();
  private CommonsHttpSolrServer solrClient;

  public PostIndexer(Path clusteredPostsPath, Path clusterPath, String outputDictionaryPattern, CommonsHttpSolrServer solrClient) throws MalformedURLException {
    this.clusteredPostsPath = clusteredPostsPath;
    this.clusterPath = clusterPath;
    this.outputDictionaryPattern = outputDictionaryPattern;
    this.solrClient = solrClient;
  }

  public void buildIndex() throws IOException, SolrServerException {
    SequenceFileDirIterable<LongWritable, ClusteredDocument> iterable = new SequenceFileDirIterable<LongWritable, ClusteredDocument>(clusteredPostsPath, PathType.GLOB, PathFilters.partFilter(), getConf());
    for (Pair<LongWritable, ClusteredDocument> pair : iterable) {
      indexPost(pair);
    }

    Configuration configuration = getConf();

    String[] dictionary = VectorHelper.loadTermDictionary(configuration, outputDictionaryPattern);

    SequenceFileDirIterable<Text, Cluster> clusterIterable = new SequenceFileDirIterable<Text, Cluster>(clusterPath, PathType.GLOB, PathFilters.partFilter(), getConf());

    for (Pair<Text, Cluster> pair : clusterIterable) {
      Cluster cluster = pair.getSecond();

      addName(clusterNames, dictionary, cluster);

      int clusterId = cluster.getId();

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", UUID.randomUUID().toString());
      doc.addField("cluster_id", clusterId);
      doc.addField("cluster_name", clusterNames.get(clusterId));
      doc.addField("size", cluster.getNumPoints());

      solrClient.add(doc);

      System.out.println("Indexed cluster " + clusterId);
    }

    solrClient.commit();
  }

  private void indexPost(Pair<LongWritable, ClusteredDocument> pair) throws IOException, SolrServerException {
    long id = pair.getFirst().get();
    ClusteredDocument clusteredDocument = pair.getSecond();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", UUID.randomUUID().toString());
    doc.addField("post_id", String.valueOf(id));
    doc.addField("post_cluster_id", clusteredDocument.getClusterId().toString());
    doc.addField("title", clusteredDocument.getDocumentTitle().toString());
    doc.addField("content", clusteredDocument.getDocumentContent().toString());

    solrClient.add(doc);

    System.out.println("Indexed post " + String.valueOf(id));
  }

  private void addName(Map<Integer, String> clusterNames, String[] dictionary, Cluster cluster) {
    List<TermIndexWeight> vectorTerms = new ArrayList<TermIndexWeight>();

    Iterator<Vector.Element> iter = cluster.getCenter().iterateNonZero();
    while (iter.hasNext()) {
      Vector.Element elt = iter.next();
      vectorTerms.add(new TermIndexWeight(elt.index(), elt.get()));
    }

    // Sort results in reverse order (ie weight in descending order)
    Collections.sort(vectorTerms, new Comparator<TermIndexWeight>() {
      @Override
      public int compare(TermIndexWeight one, TermIndexWeight two) {
        return Double.compare(two.weight, one.weight);
      }
    });

    int index = vectorTerms.get(0).index;
    String clusterName = dictionary[index];

    if (clusterName.equals("")) {
      clusterName = "Unknown";
    }

    clusterNames.put(cluster.getId(), clusterName);
  }

  private static class TermIndexWeight {
    private final int index;
    private final double weight;

    TermIndexWeight(int index, double weight) {
      this.index = index;
      this.weight = weight;
    }
  }
}
