package org.nchc.bigdata.mapreduce.image;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/12/17.
 */
public class SeqFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        for(BytesWritable bw : values) {
            context.write(key,bw);
        }
    }
}
