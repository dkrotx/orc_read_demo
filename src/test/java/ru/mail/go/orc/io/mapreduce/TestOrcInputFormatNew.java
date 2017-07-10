package ru.mail.go.orc.io.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.*;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestOrcInputFormatNew {
    private String test_dir = "test-data";
    private String test_file = test_dir + "/file.orc";
    private int nrecords = 100000;

    private boolean[] bool_array_all_zeroes(int n, int... exceptions) {
        boolean[] res = new boolean[n];

        for (int i: exceptions) {
            res[i] = true;
        }

        return res;
    }

    private boolean[] bool_array_all_ones(int n, int... exceptions) {
        boolean[] res = bool_array_all_zeroes(n, exceptions);
        for (int i = 0; i < res.length; ++i)
            res[i] = !res[i];
        return res;
    }

    @Before
    public void initDirectories() {
        File dir = new File(test_dir);
        dir.mkdir();
    }

    @Test
    public void TestEasyStripeFiltering() throws IOException {
        OrcFileWriter.WriteDemoOrcFile(test_file, nrecords);

        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(test_file), OrcFile.readerOptions(conf));
        List<StripeStatistics> stat =  reader.getStripeStatistics();

        assertThat(stat.size(), greaterThan(0));
        long desired_id;

        // select second stripe getting value of 'id' in the middle

        {
            StripeStatistics st = stat.get(1);
            IntegerColumnStatistics id_stat = ((IntegerColumnStatistics)st.getColumnStatistics()[1]);
            desired_id = (id_stat.getMinimum() + id_stat.getMaximum()) / 2;
        }

        SchemaEvolution evolution = new SchemaEvolution(reader.getSchema(), null);

        {
            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                    .equals("id", PredicateLeaf.Type.LONG, desired_id)
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);

            for (int i = 0; i < stat.size(); i++) {
                String msg = String.format("Only stripe #%d should satisfy", desired_id);
                assertThat(OrcInputFormatNew.isStripeSatisfyPredicate(stat.get(i), sarg, filterColumns, evolution),
                        equalTo(i == 1));
            }
        }

        {
            // This SearchArgument includes all stripes
            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                    .lessThan("id", PredicateLeaf.Type.LONG, nrecords + 1L)
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            for (StripeStatistics aStat : stat) {
                assertTrue("All stripes satisfy predicate",
                        OrcInputFormatNew.isStripeSatisfyPredicate(aStat, sarg, filterColumns, evolution));
            }
        }

        {
            // This SearchArgument includes none stripes
            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                    .between("id", PredicateLeaf.Type.LONG, nrecords + 1L, Long.MAX_VALUE)
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            for (StripeStatistics aStat : stat) {
                assertFalse("None stripes satisfy predicate",
                        OrcInputFormatNew.isStripeSatisfyPredicate(aStat, sarg, filterColumns, evolution));
            }
        }
    }

    @Test
    public void TestHardStripeFiltering() throws IOException {
        OrcFileWriter.WriteDemoOrcFile(test_file, nrecords);
        Path test_path = new Path(test_file);

        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(test_path, OrcFile.readerOptions(conf));
        List<StripeStatistics> stat = reader.getStripeStatistics();
        SchemaEvolution evolution = new SchemaEvolution(reader.getSchema(), null);

        assertThat(stat.size(), greaterThan(0));

        {
            // only stripe #1 satisfy predicate
            long min_id, max_id;
            long min_phone, max_phone;

            StripeStatistics st = stat.get(1);
            IntegerColumnStatistics id_stat = ((IntegerColumnStatistics) st.getColumnStatistics()[1]);
            min_id = id_stat.getMinimum();
            max_id = id_stat.getMaximum();

            IntegerColumnStatistics phone_stat = ((IntegerColumnStatistics) st.getColumnStatistics()[3]);
            min_phone = phone_stat.getMinimum();
            max_phone = phone_stat.getMaximum();

            SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd()
                    .between("id", PredicateLeaf.Type.LONG, min_id, max_id)
                    .between("phone", PredicateLeaf.Type.LONG, min_phone, max_phone)
                    .lessThan("phone", PredicateLeaf.Type.LONG, max_phone)
                    .end()
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            for (int i = 0; i < stat.size(); i++) {
                assertThat("Only stripe #1 satisfy",
                        OrcInputFormatNew.isStripeSatisfyPredicate(stat.get(i), sarg, filterColumns, evolution),
                        equalTo(i == 1));
            }

            assertThat("Only stripe #1 satisfy", bool_array_all_zeroes(stat.size(), 1),
                    equalTo(OrcInputFormatNew.pickStripesInternal(sarg, filterColumns, stat, test_path, evolution)));
        }

        {
            // none stripes satisfy because `phone' is always positive
            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                    .between("phone", PredicateLeaf.Type.LONG, -100L, -1L)
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            for (StripeStatistics aStat : stat) {
                assertFalse("None stripes satisfy",
                        OrcInputFormatNew.isStripeSatisfyPredicate(aStat, sarg, filterColumns, evolution));
            }

            assertThat("Only stripe #1 satisfy", bool_array_all_zeroes(stat.size()),
                    equalTo(OrcInputFormatNew.pickStripesInternal(sarg, filterColumns, stat, test_path, evolution)));
        }

        {
            // all stripes satisfy predicate because of OR expression
            SearchArgument sarg = SearchArgumentFactory.newBuilder().startOr()
                        .startAnd()
                        .between("phone", PredicateLeaf.Type.LONG, -100L, -1L)
                        .end()
                    .lessThan("id", PredicateLeaf.Type.LONG, nrecords + 1L)
                    .end()
                    .build();

            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sarg.getLeaves(), evolution);
            for (StripeStatistics aStat : stat) {
                assertTrue("Only stripe #1 satisfy",
                        OrcInputFormatNew.isStripeSatisfyPredicate(aStat, sarg, filterColumns, evolution));
            }

            assertThat("Only stripe #1 satisfy", bool_array_all_ones(stat.size()),
                    equalTo(OrcInputFormatNew.pickStripesInternal(sarg, filterColumns, stat, test_path, evolution)));
        }
    }

    @Test
    public void TestSplitGeneratorEasy() throws IOException {
        List<StripeInformation> stripes = new ArrayList<>();
        stripes.add(new MockedStripeInformation(0, 1000));
        stripes.add(new MockedStripeInformation(1000, 1000));
        stripes.add(new MockedStripeInformation(2000, 1000));

        BlockLocation[] blocks = new BlockLocation[] {
            new BlockLocation(null, new String[]{"host1"}, 0, 1000),
            new BlockLocation(null, new String[]{"host2"}, 1000, 1000),
            new BlockLocation(null, new String[]{"host3"}, 2000, 1000)
        };

        {
            // include only first stripe
            List<FileSplit> splits = new OrcInputFormatNew.SplitsGenerator(stripes,
                    bool_array_all_zeroes(stripes.size(), 0),
                    blocks, new Path(test_file)).generateSplits();

            assertThat(splits.size(), equalTo(1));
            FileSplit s = splits.get(0);
            assertThat(s.getStart(), equalTo(0L));
            assertThat(s.getLength(), equalTo(1000L));
            assertThat(s.getLocations(), equalTo(new String[]{"host1"}));
        }

        {
            // include first and last stripe
            List<FileSplit> splits = new OrcInputFormatNew.SplitsGenerator(stripes,
                    bool_array_all_ones(stripes.size(), 1),
                    blocks, new Path(test_file)).generateSplits();

            assertThat(splits.size(), equalTo(2));

            assertThat(splits.get(0).getStart(), equalTo(0L));
            assertThat(splits.get(0).getLength(), equalTo(1000L));
            assertThat(splits.get(0).getLocations(), equalTo(new String[]{"host1"}));

            assertThat(splits.get(1).getStart(), equalTo(2000L));
            assertThat(splits.get(1).getLength(), equalTo(1000L));
            assertThat(splits.get(1).getLocations(), equalTo(new String[]{"host3"}));
        }

        {
            // include none stripes
            List<FileSplit> splits = new OrcInputFormatNew.SplitsGenerator(stripes,
                    bool_array_all_zeroes(stripes.size()),
                    blocks, new Path(test_file)).generateSplits();

            assertTrue(splits.isEmpty());
        }
    }

    @Test
    public void TestSplitGeneratorHard() throws IOException {
        List<StripeInformation> stripes = new ArrayList<>();
        stripes.add(new MockedStripeInformation(0, 1000));
        stripes.add(new MockedStripeInformation(1000, 1000));
        stripes.add(new MockedStripeInformation(2000, 1000));

        BlockLocation[] blocks = new BlockLocation[] {
                new BlockLocation(null, new String[]{"host1"}, 0, 100),
                new BlockLocation(null, new String[]{"host2", "host3", "host4"}, 100, 900),
                new BlockLocation(null, new String[]{"host3"}, 1000, 1000),
                new BlockLocation(null, new String[]{"host4"}, 2000, 600),
                new BlockLocation(null, new String[]{"host5", "host6", "host7"}, 2600, 10000)
        };

        {
            // include first and last stripe
            List<FileSplit> splits = new OrcInputFormatNew.SplitsGenerator(stripes,
                    bool_array_all_ones(stripes.size(), 1),
                    blocks, new Path(test_file)).generateSplits();

            // Location host1 is bad because it has only 10% of block data
            // so second location should be taken. With proper [non-local] start.
            // Similarly, location host5,6,7 is bad - host4 contains 60% of stripe

            assertThat(splits.size(), equalTo(2));

            assertThat(splits.get(0).getStart(), equalTo(0L));
            assertThat(splits.get(0).getLength(), equalTo(1000L));
            assertThat(splits.get(0).getLocations(), equalTo(new String[]{"host2", "host3", "host4"}));

            assertThat(splits.get(1).getStart(), equalTo(2000L));
            assertThat(splits.get(1).getLength(), equalTo(1000L));
            assertThat(splits.get(1).getLocations(), equalTo(new String[]{"host4"}));
        }
    }

    @Test (expected = OrcInputFormatNew.SplitsGenerator.NoSuchBlockException.class)
    public void TestDifferentBlocksAndStripes() throws IOException {
        // in case of none blocks overlapped with stripes exception should be thrown
        // just because it's global error somewhere

        List<StripeInformation> stripes = new ArrayList<>();
        stripes.add(new MockedStripeInformation(0, 1000));
        stripes.add(new MockedStripeInformation(1000, 1000));
        stripes.add(new MockedStripeInformation(2000, 1000));

        BlockLocation[] blocks = new BlockLocation[] {
                new BlockLocation(null, new String[]{"host1"}, 0, 1000),
                new BlockLocation(null, new String[]{"host2"}, 1000, 1000),
                // no block offering offset >= 2000
        };

        List<FileSplit> splits = new OrcInputFormatNew.SplitsGenerator(stripes,
                bool_array_all_ones(stripes.size()),
                blocks, new Path(test_file)).generateSplits();
    }
}
