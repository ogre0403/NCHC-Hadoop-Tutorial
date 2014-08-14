package org.nchc.bigdata.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by 1403035 on 2014/8/12.
 */
public class SeqFileTest {

    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        Path path = new Path("/test.seq");
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        SequenceFile.Writer writer =
                SequenceFile.createWriter(path.getFileSystem(conf), conf, path,
                        new Text().getClass(), new BytesWritable().getClass());

        appendToSeqFile(new File("/D:/kafka_image/"),writer);

        writer.close();
    }

    private static void appendToSeqFile(File folder, SequenceFile.Writer writer) throws IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                appendToSeqFile(fileEntry, writer);
            } else {
                appendImage(fileEntry.getCanonicalPath(), writer);
            }
        }
    }

    private static void appendImage(String fname, SequenceFile.Writer writer) throws IOException {
        String[] resultTokens = fname.split("\\\\");
        Text k = new Text(resultTokens[resultTokens.length-1]);
        FileInputStream fis = new FileInputStream(fname);
        byte[] ba = IOUtils.toByteArray(fis);
        BytesWritable v = new BytesWritable(ba);
        writer.append(k,v);
    }
}
