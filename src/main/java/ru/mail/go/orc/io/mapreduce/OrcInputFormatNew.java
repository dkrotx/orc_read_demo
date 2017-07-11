package ru.mail.go.orc.io.mapreduce;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.*;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class OrcInputFormatNew extends OrcInputFormat<OrcStruct> {
    private static final Logger LOG = LoggerFactory.getLogger(OrcInputFormatNew.class);
    private static final String ORC_INPUTFORMAT_OPTS = "orc.inputformat_new.opts";

    /**
     * Check whatever given stripe is satisfying filter predicate
     *
     * @param stripeStatistics stripe statistics (min/max is used)
     * @param sarg             filter
     * @param filterColumns    columns IDs for leaves of filter
     * @param evolution        schema evolution for reading
     * @return true if this stripe is candidate for filter
     */
    static boolean isStripeSatisfyPredicate(
            StripeStatistics stripeStatistics, SearchArgument sarg, int[] filterColumns,
            final SchemaEvolution evolution) {
        List<PredicateLeaf> predLeaves = sarg.getLeaves();
        SearchArgument.TruthValue[] truthValues = new SearchArgument.TruthValue[predLeaves.size()];

        for (int pred = 0; pred < truthValues.length; pred++) {
            if (filterColumns[pred] != -1) {
                if (evolution != null && !evolution.isPPDSafeConversion(filterColumns[pred])) {
                    truthValues[pred] = SearchArgument.TruthValue.YES_NO_NULL;
                } else {
                    // column statistics at index 0 contains only the number of rows
                    ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
                    truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, predLeaves.get(pred), null);
                }
            } else {
                // parition column case.
                // partition filter will be evaluated by partition pruner so
                // we will not evaluate partition filter here.
                truthValues[pred] = SearchArgument.TruthValue.YES_NO_NULL;
            }
        }
        return sarg.evaluate(truthValues).isNeeded();
    }

    static boolean[] pickStripesInternal(SearchArgument sarg, int[] filterColumns,
                                         List<StripeStatistics> stripeStats,
                                         Path filePath, final SchemaEvolution evolution) {
        boolean[] includeStripe = new boolean[stripeStats.size()];

        for (int i = 0; i < includeStripe.length; ++i) {
            includeStripe[i] = isStripeSatisfyPredicate(stripeStats.get(i), sarg, filterColumns, evolution);

            if (!includeStripe[i]) {
                LOG.debug("Eliminating ORC stripe-" + i + " of file '" + filePath
                        + "'  as it did not satisfy predicate condition.");
            }
        }
        return includeStripe;
    }

    /**
     * Set options controlling the way input splits created
     *
     * @param conf Hadoop configuration to serialize options to
     * @param opts options itself
     */
    static public void setInputFormatOptions(Configuration conf, Options opts) {
        try {
            conf.set(ORC_INPUTFORMAT_OPTS, SerializationUtils.SerializeBase64(opts));
        } catch (IOException e) {
            throw new RuntimeException("setInputFormatOptions - no memory?");
        }
    }

    /**
     * Retrieve saved input format options
     *
     * @param conf Hadoop configuration to deserialize options from
     * @return options object
     */
    static public Options getInputFormatOptions(Configuration conf) {
        String opts_str = conf.get(ORC_INPUTFORMAT_OPTS);

        if (opts_str != null) {
            try {
                return (Options) SerializationUtils.DeserializeFromBase64(opts_str);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("getInputFormatOptions - bad string in conf?");
            }
        }

        return Options.Make();
    }

    @Override
    public List<InputSplit> getSplits(JobContext ctx) throws IOException {
        Configuration conf = ctx.getConfiguration();
        Options opts = getInputFormatOptions(conf);

        try {
            if (opts.read_meta) {
                SearchArgument sarg = extractSearchArgument(conf);
                return getSplitsParallel(conf, opts, sarg, listStatus(ctx));
            }
        } catch (InterruptedException e) {
            LOG.error("Failed to get proper splits in parallel. Fallback to default approach");
        }

        return getDefaultSplits(ctx);
    }

    /**
     * Get splits for given fileStatuses in parallel.
     * It has significant boost because getting information from HDFS NameNode
     * requires much time and produce network IO.
     *
     * @param conf         Hadoop configuration
     * @param opts         options controlling how much parallel instances to fetch
     * @param sarg         SearchArgument to filter only meaningful ORC-stripes
     * @param fileStatuses ORC files to make splits for
     * @return splits for meaningful stripes for each file
     * @throws InterruptedException in case of some interruptions (method itself waits forever)
     */
    private List<InputSplit> getSplitsParallel(Configuration conf, Options opts, SearchArgument sarg,
                                               List<FileStatus> fileStatuses) throws InterruptedException {
        ArrayList<Callable<List<FileSplit>>> tasks = new ArrayList<>();
        fileStatuses.forEach(st -> tasks.add(() -> getSplitForFile(conf, st, sarg)));

        ExecutorService tpool = Executors.newFixedThreadPool(opts.split_builders_parallel);
        List<Future<List<FileSplit>>> futures = tpool.invokeAll(tasks);

        ArrayList<InputSplit> result = new ArrayList<>();

        try {
            for (Future<List<FileSplit>> f : futures)
                result.addAll(f.get());
        } catch (ExecutionException e) {
            throw new InterruptedException("getSplitsParallel: actual split is not ready");
        }

        return result;
    }

    /**
     * Return default (by HDFS block) splits for all given files
     *
     * @param ctx context of job
     * @return split for every block of each file
     * @throws IOException
     */
    private List<InputSplit> getDefaultSplits(JobContext ctx) throws IOException {
        return super.getSplits(ctx);
    }

    /**
     * Get split fo concrete file
     *
     * @param conf   Hadoop configuration
     * @param status file status
     * @param sarg   SearchArgument to filter only meaningful ORC-stripes
     * @return list of input splits for meaningful stripes
     * @throws IOException in case of HDFS problems
     */
    private List<FileSplit> getSplitForFile(Configuration conf, FileStatus status, SearchArgument sarg) throws IOException {
        Path path = status.getPath();
        FileSystem fs = path.getFileSystem(conf);
        BlockLocation[] dfs_blocks = fs.getFileBlockLocations(status, 0, status.getLen());
        OrcFileInfo orc_info = OrcFileInfo.FromFile(conf, path);

        boolean[] included_stripes = getIncludedStripes(orc_info.schema, sarg, orc_info.stripe_stat, path);
        SplitsGenerator sgen = new SplitsGenerator(orc_info.stripes, included_stripes, dfs_blocks, path);

        List<FileSplit> splits = sgen.generateSplits();
        printSkipStatistics(status, splits);

        return splits;
    }

    /**
     * Print how much [blocks] we will skip for given file
     *
     * @param status ORC file
     * @param splits built splits for this file
     */
    private void printSkipStatistics(FileStatus status, List<FileSplit> splits) {
        long splits_len = summSplitsLen(splits);
        double percent = status.getLen() == 0 ? 0.0 :
                (1.0 - (double) splits_len / status.getLen()) * 100.0;

        LOG.info(String.format("ORC: skipped %.1f%% percent for '%s'",
                percent, status.getPath()));
    }

    /**
     * Get total length of splits
     *
     * @param splits built split for file
     * @return sum
     */
    private long summSplitsLen(List<FileSplit> splits) {
        long sum = 0;
        for (FileSplit split : splits) {
            sum += split.getLength();
        }
        return sum;
    }

    /**
     * Calculate mask stripes matching specific filter
     *
     * @param schema      ORC file schema
     * @param sarg        filter
     * @param stripe_stat stripe statistics
     * @param path        path to HDFS file
     * @return mask-array where true means "this stripe matches filter"
     */
    private boolean[] getIncludedStripes(TypeDescription schema,
                                         SearchArgument sarg, List<StripeStatistics> stripe_stat, Path path) {
        if (sarg != null) {
            SchemaEvolution evolution = new SchemaEvolution(schema, null);
            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            return pickStripesInternal(sarg, filterColumns, stripe_stat, path, evolution);
        }

        boolean[] all_included = new boolean[stripe_stat.size()];
        Arrays.fill(all_included, true);
        return all_included;
    }

    /**
     * Extract (serialized) search argument from conf
     *
     * @param conf Hadoop configuration
     * @return SearchArgument object
     */
    private SearchArgument extractSearchArgument(Configuration conf) {
        String kryoSarg = OrcConf.KRYO_SARG.getString(conf);

        if (kryoSarg != null) {
            byte[] sargBytes = Base64.decodeBase64(kryoSarg);
            return new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
        }
        return null;
    }

    /**
     * create reader of ORC records (invokes on mappers-side)
     *
     * @param inputSplit         split
     * @param taskAttemptContext task attemp context (from MapReduce framework)
     * @return RecordReader which provides OrcStruct as values (no keys)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, OrcStruct> createRecordReader(InputSplit inputSplit,
                                                                    TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        return super.createRecordReader(inputSplit, taskAttemptContext);
    }

    /***
     * Options for InputFormat controlling speed and resources
     */
    static public class Options implements Serializable {
        private boolean read_meta;
        private int split_builders_parallel;

        public Options() {
            read_meta = true;
            split_builders_parallel = 32;
        }

        public static Options Make() {
            return new Options();
        }

        public Options WithReadMeta(boolean read_meta) {
            this.read_meta = read_meta;
            return this;
        }

        public Options WithParallelSplitBuilders(int nparallel) {
            this.split_builders_parallel = nparallel;
            return this;
        }
    }

    /**
     * Splits generator which abstract dealing with ORC stripes and HDFS blocks
     */
    static class SplitsGenerator {
        private final Path path;
        private final List<StripeInformation> stripes;
        private final boolean[] included_stripes;
        private final BlockLocation[] blocks;

        SplitsGenerator(List<StripeInformation> stripes, boolean[] included_stripes,
                        BlockLocation[] blocks,
                        Path path) {
            this.stripes = stripes;
            this.included_stripes = included_stripes;
            this.blocks = blocks;
            this.path = path;
        }

        List<FileSplit> generateSplits() throws IOException {
            // assume that stripe is big enough and separate split
            // is OK for distinct stripe
            ArrayList<FileSplit> splits = new ArrayList<>();

            for (int i = 0; i < stripes.size(); i++) {
                boolean included = (included_stripes == null || i >= included_stripes.length) || included_stripes[i];
                if (included) {
                    splits.add(findBestBlock(stripes.get(i)));
                }
            }

            return splits;
        }

        private FileSplit findBestBlock(StripeInformation si) throws IOException {
            BlockLocation best_block = null;
            long best_overlap = 0;

            for (BlockLocation blk : blocks) {
                long overlap = getOverlap(blk.getOffset(), blk.getLength(), si.getOffset(), si.getLength());
                if (overlap > best_overlap) {
                    best_block = blk;
                    best_overlap = overlap;
                }
            }

            if (best_block == null) {
                throw new NoSuchBlockException(String.format("Cant find block for stripe (offset: %d; len: %d)",
                        si.getOffset(), si.getLength()));
            }

            return new FileSplit(path, si.getOffset(), si.getLength(), best_block.getHosts());
        }

        private long getOverlap(long block_offset, long block_length, long offset, long length) {
            long max_begin = Math.max(block_offset, offset);
            long min_end = Math.min(block_offset + block_length, offset + length);

            return (min_end <= max_begin) ? 0 : min_end - max_begin;
        }

        class NoSuchBlockException extends IOException {
            NoSuchBlockException(String err) {
                super(err);
            }
        }
    }

    /**
     * Structure holding usefull information about single ORC file
     */
    static class OrcFileInfo {
        final List<StripeStatistics> stripe_stat;
        final List<StripeInformation> stripes;
        final TypeDescription schema;

        private OrcFileInfo(List<StripeStatistics> stripe_stat,
                            List<StripeInformation> stripes, TypeDescription schema) {
            this.stripe_stat = stripe_stat;
            this.stripes = stripes;
            this.schema = schema;
        }

        static OrcFileInfo FromFile(Configuration conf, Path path) throws IOException {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            return new OrcFileInfo(reader.getStripeStatistics(),
                    reader.getStripes(), reader.getSchema());
        }
    }
}
