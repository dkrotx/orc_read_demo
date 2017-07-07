import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.mapred.OrcStruct;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;


public class DataFrameDemo {
    public static void run(String path) throws IOException {
        RecordSchema record_schema = new RecordSchema();
        SparkConf conf = new SparkConf().setAppName(DataFrameDemo.class.getCanonicalName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hconf = sc.hadoopConfiguration();
        Job job = Job.getInstance(hconf);
        SQLContext sqlContext = new SQLContext(sc);


        OffloadedOrcInputFormat.addInputPath(job, new Path(path));
        OffloadedOrcInputFormat.setSearchArgument(job.getConfiguration(),
                SearchArgumentFactory.newBuilder()
                        .equals("id", PredicateLeaf.Type.LONG, 25603L)
                        .build(),
                new String[]{null, "id", "name", "phone"});

        JavaRDD<OrcStruct> orc_rows = sc.newAPIHadoopRDD(job.getConfiguration(),
                OffloadedOrcInputFormat.class,
                NullWritable.class, OrcStruct.class).values();

        JavaRDD<Row> rowRDD = orc_rows.map(RecordSchema::OrcStruct2SQLRow);
        DataFrame df = sqlContext.createDataFrame(rowRDD, record_schema.GetSQLSchema());

        df.select(df.col("id"), df.col("name")).show();
        //df.select(df.col("id"), df.col("name")).filter(df.col("id").gt(99)).show();

    }

    public static void main(String[] args) throws IOException {
        DataFrameDemo.run(args[0]);
    }
}