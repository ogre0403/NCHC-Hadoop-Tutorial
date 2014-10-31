package org.nchc.bigdata.mapreduce.LOG.analyze;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/10/6.
 */
public class LogMapper extends Mapper<Object, Text, Entry, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static Logger logger = Logger.getLogger(LogMapper.class);
    private Entry entry = new Entry();
    private StringBuilder sb = new StringBuilder();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String newline = value.toString();
        logger.info("input sentence [" + newline+"]");
        String[] sa = newline.split(" ");

        if(sa.length < 3)
            return;

        if (!sa[2].equalsIgnoreCase("error")){
            return;
        }else{
            entry.setClass(sa[3]);
            entry.setException(sa[4]);
        }

        logger.info(entry);
        context.write(entry,one);
    }
}