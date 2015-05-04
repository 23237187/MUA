import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.FileReader;


class CSVToMahout {

    private CSVToMahout() {}

    public static final int NUM_COLUMNS = 4;

    public static void main(String[] args) throws Exception
    {

        String INPUT_FILE =  "/ZTE_Demo/SKM_Iterations/centroids.csv";
        String OUTPUT_FILE = "/ZTE_Demo/SKM_Iterations/centroids";

        List<NamedVector> centroids = new ArrayList<NamedVector>();
        NamedVector centroid;
        BufferedReader br = null;
        br = new BufferedReader(new FileReader(INPUT_FILE));
        String sCurrentLine;
        while ((sCurrentLine = br.readLine()) != null) {

            String item_name = sCurrentLine.split(",")[0];
            double[] features = new double[NUM_COLUMNS-1];
            for(int indx=1; indx<NUM_COLUMNS;++indx){
                features[indx-1] = Float.parseFloat(sCurrentLine.split(",")[indx]);
            }

            centroid = new NamedVector(new DenseVector(features), item_name );
            centroids.add(centroid);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(OUTPUT_FILE);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);

        VectorWritable vec = new VectorWritable();
        for(NamedVector vector : centroids){
            vec.set(vector);
            writer.append(new Text(vector.getName()), vec);
        }
        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT_FILE), conf);

        Text key = new Text();
        VectorWritable value = new VectorWritable();
        while(reader.next(key, value)){
            System.out.println(key.toString() + " , " + value.get().asFormatString());
        }
        reader.close();
    }
}
