package org.nchc.bigdata.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nchc.bigdata.mapreduce.LOG.analyze.Entry;
import org.nchc.bigdata.mapreduce.LOG.analyze.LogMapper;
import org.nchc.bigdata.mapreduce.LOG.analyze.LogReducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 1403035 on 2014/10/7.
 */
public class LogAnalyzeTest {

    private Mapper<Object, Text, Entry, IntWritable> mapper;
    private MapDriver<Object, Text, Entry, IntWritable> mapdriver;
    private Reducer<Entry,IntWritable,Entry,IntWritable> reducer;
    private ReduceDriver<Entry,IntWritable,Entry,IntWritable> reducerdriver;
    private MapReduceDriver<Object, Text, Entry, IntWritable, Entry,IntWritable> MRdriver;

    private static Logger logger = Logger.getLogger(LogAnalyzeTest.class);
    @Before
    public void init() {
        mapper = new LogMapper();
        mapdriver = new MapDriver<Object, Text, Entry, IntWritable>(mapper);

        reducer = new LogReducer();
        reducerdriver = new ReduceDriver<Entry,IntWritable,Entry,IntWritable>(reducer);

        MRdriver = new MapReduceDriver<Object, Text, Entry, IntWritable, Entry,IntWritable>(mapper, reducer);
    }

    @Test
    public void testEntry() {
        Entry A = new Entry("class1", "Exception1");
        Entry B = new Entry("class1", "Exception1");
        Assert.assertTrue(A.equals(B));
        Assert.assertTrue(A.compareTo(B) == 0);
        logger.info(A);
    }

    @Test
    public void testMapper() throws IOException {
        String line1 = "2014-10-06 08:45:43,230 INFO org.nchc.bigdata.mapreduce.LOG.generator.LogThread1: NORMAL";
        String line2 = "2014-10-06 08:46:53,820 ERROR org.nchc.bigdata.mapreduce.LOG.generator.LogThread2: java.lang.ClassNotFoundException";
        String line3 = "    at org.nchc.bigdata.mapreduce.LOG.generator.LogThread2.ExceptionGen(LogThread2.java:47)";
        String line4 = "    at org.nchc.bigdata.mapreduce.LOG.generator.LogThread2.run(LogThread2.java:27)";
        String line5 = "    at java.lang.Thread.run(Thread.java:744)";
        String line6 = "2014-10-06 08:46:53,822 INFO org.nchc.bigdata.mapreduce.LOG.generator.LogThread2: NORMAL";
        mapdriver
                .withInput(new Text(""), new Text(line1))
                .withInput(new Text(""), new Text(line2))
                .withInput(new Text(""), new Text(line3))
                .withInput(new Text(""), new Text(line4))
                .withInput(new Text(""), new Text(line5))
                .withInput(new Text(""), new Text(line6))
                .withOutput(new Entry("org.nchc.bigdata.mapreduce.LOG.generator.LogThread2:",
                                        "java.lang.ClassNotFoundException"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducerr() throws IOException {
        String c = "Class1";
        String e = "Exception1";
        ArrayList<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reducerdriver
                .withInput(new Entry(c,e), values)
                .withOutput(new Entry(c,e), new IntWritable(2))
                .runTest();
    }
}
