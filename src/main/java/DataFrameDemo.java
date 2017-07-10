import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import ru.mail.go.orc.io.mapreduce.OrcInputFormatNew;
import ru.mail.go.orc.io.spark.OrcRecordSchema;

import java.io.IOException;


public class DataFrameDemo {
    public static void run(String path, int id) throws IOException {
        OrcRecordSchema record_schema = PhoneBookRecord.MakeOrcRecordSchema();
        SparkConf conf = new SparkConf().setAppName(DataFrameDemo.class.getCanonicalName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hconf = sc.hadoopConfiguration();
        Job job = Job.getInstance(hconf);
        SQLContext sqlContext = new SQLContext(sc);


        OrcInputFormatNew.addInputPath(job, new Path(path));
        OrcInputFormatNew.setSearchArgument(job.getConfiguration(),
                SearchArgumentFactory.newBuilder()
                        .equals("id", PredicateLeaf.Type.LONG, (long)id)
                        .build(),
                new String[]{null, "id", "name", "phone"});

        JavaRDD<OrcStruct> orc_rows = sc.newAPIHadoopRDD(job.getConfiguration(),
                OrcInputFormatNew.class,
                NullWritable.class, OrcStruct.class).values();

        JavaRDD<Row> rowRDD = orc_rows.map(OrcRecordSchema::OrcStruct2SQLRow);
        DataFrame df = sqlContext.createDataFrame(rowRDD, record_schema.GetSQLSchema());

        df.select(df.col("id"), df.col("name"))
                .filter(df.col("id").equalTo(id))
                .show();

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        DataFrameDemo.run(args[0], Integer.valueOf(args[1]));
    }
}