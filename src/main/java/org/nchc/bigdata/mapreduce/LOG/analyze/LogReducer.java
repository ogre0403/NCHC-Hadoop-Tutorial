package org.nchc.bigdata.mapreduce.LOG.analyze;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/10/6.
 */
public class LogReducer extends Reducer<Entry,IntWritable,Entry,IntWritable> {

    private static Logger logger = Logger.getLogger(LogReducer.class);
    public void reduce(Entry key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        logger.info("KEY = " + key);
        int sum = 0;
        IntWritable result = new IntWritable();
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
