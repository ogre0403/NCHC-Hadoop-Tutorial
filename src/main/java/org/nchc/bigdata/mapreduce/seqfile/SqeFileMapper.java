package org.nchc.bigdata.mapreduce.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SqeFileMapper extends Mapper<NullWritable, BytesWritable, NullWritable, NullWritable> {

    private static FileSystem fs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int sss = value.getLength();
        byte[] ba = new byte[sss];
        System.arraycopy(value.getBytes(),0,ba,0,sss);

//      Do somethong for the image

        long rand = System.currentTimeMillis();
        Path out = new Path("/out/result_"+rand+".jpg");
        OutputStream os = fs.create(out);
        ByteArrayInputStream is = new ByteArrayInputStream(ba);
        org.apache.hadoop.io.IOUtils.copyBytes(is, os, conf);
    }
}
