import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;


public class DataFrameDemo {
    public static void run(String path, String out) throws IOException {
        SparkConf conf = new SparkConf().setAppName(DataFrameDemo.class.getCanonicalName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hconf = sc.hadoopConfiguration();
        Job job = Job.getInstance(hconf);

        OffloadedOrcInputFormat.addInputPath(job, new Path(path));
        JavaRDD<OrcStruct> orc_rows = sc.newAPIHadoopRDD(job.getConfiguration(),
                OffloadedOrcInputFormat.class,
                NullWritable.class, OrcStruct.class).values();
        orc_rows.map(row -> row.toString()).saveAsTextFile(out);
    }

    public static void main(String[] args) throws IOException {
        DataFrameDemo.run(args[0], args[1]);
    }
}