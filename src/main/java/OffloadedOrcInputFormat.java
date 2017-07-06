import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OffloadedOrcInputFormat extends OrcInputFormat<OrcStruct> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        for (FileStatus status: listStatus(context)) {
            splits.add(getSplitForFile(status));
        }

        return splits;
    }

    private InputSplit getSplitForFile(FileStatus status) {
        return new FileSplit(status.getPath(), 0, status.getLen(), new String[] {});
    }
}
