package org.nchc.train.mr.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SqeFileMapper extends Mapper<Text, BytesWritable, NullWritable, NullWritable> {

    private static FileSystem fs;
    private static Configuration conf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fs = FileSystem.get(context.getConfiguration());
        conf = context.getConfiguration();
    }

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        int sss = value.getLength();
        byte[] ba = new byte[sss];
        System.arraycopy(value.getBytes(),0,ba,0,sss);

//      Do somethong for the image
        writeToHDFS(key.toString(),ba);

    }

    private void writeToHDFS(String path, byte[] ba) throws IOException {
        Path out = new Path("/out/"+path);
        OutputStream os = fs.create(out);
        ByteArrayInputStream is = new ByteArrayInputStream(ba);
        org.apache.hadoop.io.IOUtils.copyBytes(is, os, conf);
    }
}
