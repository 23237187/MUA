import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;

import java.io.IOException;
public class seqReader {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/ZTE_Demo/cluster_raw"), conf);
        IntWritable key = new IntWritable();
        ClusterWritable value = new ClusterWritable();
        while(reader.next(key, value)){
            System.out.println(key.toString() + " , " + value.toString());
        }
        reader.close();
    }
}
