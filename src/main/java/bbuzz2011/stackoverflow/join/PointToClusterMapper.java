package bbuzz2011.stackoverflow.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

import java.io.IOException;

/**
 * Maps the name of a point to the id of the cluster, sorted by point name in ascending order.
 */
public class PointToClusterMapper extends Mapper<IntWritable, WeightedVectorWritable, LongWritable, IntWritable> {

  private LongWritable pointName = new LongWritable();

  @Override
  protected void map(IntWritable clusterId, WeightedVectorWritable point, Context context) throws IOException, InterruptedException {
    NamedVector namedVector;
    if (point.getVector() instanceof NamedVector) {
      namedVector = (NamedVector) point.getVector();
    } else {
      throw new RuntimeException("Cannot output point name, point is not a NamedVector");
    }

    pointName.set(Long.valueOf(namedVector.getName()));

    context.write(pointName, clusterId);
  }
}
