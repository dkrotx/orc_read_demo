package ru.mail.go.orc.io.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.Closeable;
import java.io.IOException;

class OrcFileWriter implements Closeable {
    private Writer writer;
    private VectorizedRowBatch batch;
    Path path;

    private void CreateWriter(Configuration conf, Path path) throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
                .addField("id", TypeDescription.createInt())
                .addField("name", TypeDescription.createString())
                .addField("phone", TypeDescription.createLong());

        writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).
                compress(CompressionKind.NONE).stripeSize(64 * 1024).setSchema(schema));
        batch = schema.createRowBatch();
    }

    public OrcFileWriter(String filename) {
        path = new Path(filename);
    }

    public void Write(int n) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = path.getFileSystem(conf);

        if (fs.exists(path))
            fs.delete(path, false);

        CreateWriter(conf, path);

        for (int i = 1; i <= n; i++) {
            Append(i, String.format("Name_%d", i), i * 1000);
        }

        close();
    }

    private void Append(int id, String name, long phone) throws IOException {
        LongColumnVector v_id = (LongColumnVector) batch.cols[0];
        BytesColumnVector v_name = (BytesColumnVector) batch.cols[1];
        LongColumnVector v_phone = (LongColumnVector) batch.cols[2];

        int i = batch.size;

        v_id.vector[i] = id;
        v_name.setVal(i, name.getBytes());
        v_phone.vector[i] = phone;

        if (++batch.size == batch.getMaxSize()) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        writer.close();
    }

    private void flush() throws IOException {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    static void WriteDemoOrcFile(String filename, int nrecords) {
        try {
            new OrcFileWriter(filename).Write(nrecords);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}