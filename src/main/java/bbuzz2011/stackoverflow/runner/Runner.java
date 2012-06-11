package bbuzz2011.stackoverflow.runner;

import bbuzz2011.stackoverflow.index.PostIndexer;
import bbuzz2011.stackoverflow.join.ClusterJoinerJob;
import bbuzz2011.stackoverflow.join.PointToClusterMappingJob;
import bbuzz2011.stackoverflow.preprocess.text.StackOverflowPostTextExtracterJob;
import bbuzz2011.stackoverflow.preprocess.xml.StackOverflowPostXMLParserJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

import java.io.IOException;

public class Runner {

    private Configuration configuration = new Configuration();
    private Path outputBasePath = new Path("target/stackoverflow-output-base/");

    private CommonsHttpSolrServer solrClient;
    private String outputDictionaryPattern;
    private Path outputPostsPath;
    private Path outputSeq2SparsePath;
    private Path outputVectorPath;

    public static void main(String[] args) throws Exception {
        Runner runner = new Runner();
        runner.run();
    }

    private void run() throws Exception {
        cleanOutputBasePath();
        startSolr();
        preProcess();
        vectorize();
        cluster();
        postProcess();
    }

    private void cleanOutputBasePath() throws IOException {
        HadoopUtil.delete(configuration, outputBasePath);
    }

    private void startSolr() throws Exception {
        String context = "/core0";
        int port = 8983;

        System.setProperty("solr.solr.home", "src/main/solr");
        JettySolrRunner solr = new JettySolrRunner("/", port);
        solr.start(false);

        solrClient = new CommonsHttpSolrServer("http://localhost:" + port + context);
        solrClient.deleteByQuery("*:*");
    }

    private void preProcess() throws ClassNotFoundException, IOException, InterruptedException {
        outputSeq2SparsePath = new Path(outputBasePath, "sparse");
        outputVectorPath = new Path(outputSeq2SparsePath, "tfidf-vectors");
        outputDictionaryPattern = new Path(outputSeq2SparsePath, "dictionary.file-*").toString();

        configuration.set(StackOverflowPostXMLParserJob.INPUT, "src/main/resources/posts-small.xml");
        configuration.set(StackOverflowPostXMLParserJob.OUTPUT, outputBasePath.toString());

        StackOverflowPostXMLParserJob parseJob = new StackOverflowPostXMLParserJob(configuration);
        outputPostsPath = parseJob.parseXML();

        configuration.set(StackOverflowPostTextExtracterJob.INPUT, new Path(outputBasePath, StackOverflowPostXMLParserJob.OUTPUT_POSTS_PATH).toString());
        configuration.set(StackOverflowPostTextExtracterJob.OUTPUT, outputBasePath.toString());

        StackOverflowPostTextExtracterJob extracterJob = new StackOverflowPostTextExtracterJob(configuration);
        extracterJob.run();
    }

    private void vectorize() throws Exception {
        String[] seq2SparseArgs = new String[]{
                "--input", new Path(outputBasePath, StackOverflowPostTextExtracterJob.OUTPUT_POSTS_TEXT).toString(),
                "--output", outputSeq2SparsePath.toString(),
                "--maxNGramSize", "2",
                "--namedVector",
                "--maxDFPercent", "25",
                "--norm", "2",
                "--analyzerName", "bbuzz2011.stackoverflow.StackOverflowAnalyzer",
                "--overwrite"
        };

        ToolRunner.run(configuration, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    }

    private void cluster() throws Exception {
        String algorithmSuffix = "kmeans";

        Path outputKMeansPath = new Path(outputBasePath, algorithmSuffix);

        String[] kmeansDriver = {
                "--input", outputVectorPath.toString(),
                "--output", outputKMeansPath.toString(),
                "--clusters", "target/stackoverflow-kmeans-initial-clusters",
                "--maxIter", "10",
                "--numClusters", "250",
                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
                "--clustering",
                "--method", "sequential",
                "--overwrite"
        };

        ToolRunner.run(configuration, new KMeansDriver(), kmeansDriver);
    }

    private void postProcess() throws Exception {
        Path outputClusteringPath = new Path(outputBasePath, "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path outputFinalClustersPath = new Path(outputClusteringPath, "clusters-*-final/*");
        Path pointsToClusterPath = new Path(outputBasePath, "pointsToClusters");
        Path clusteredPostsPath = new Path(outputBasePath, "clusteredPosts");

        PointToClusterMappingJob pointsToClusterMappingJob = new PointToClusterMappingJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(configuration);
        pointsToClusterMappingJob.mapPointsToClusters();

        ClusterJoinerJob clusterJoinerJob = new ClusterJoinerJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(configuration);
        clusterJoinerJob.run();

        PostIndexer postIndexer = new PostIndexer(clusteredPostsPath, outputFinalClustersPath, outputDictionaryPattern, solrClient);
        postIndexer.setConf(configuration);
        postIndexer.buildIndex();
    }
}
