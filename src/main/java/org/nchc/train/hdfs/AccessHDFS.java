package org.nchc.train.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;

/**
 * Created by 1403035 on 2014/8/6.
 */
public class AccessHDFS {
    private static FileSystem fs;
    private static Configuration conf;
    private static String hdfsURI = "hdfs://192.168.56.10:9000";

    public static void main(String[] args) throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", hdfsURI);
        fs = FileSystem.get(conf);

        writeToHDFS("A.jpg", "/a.JPG");
        readFromHDFS("/AAA.JPG","/D:/aaa.jpg");
        String[] names = {"A.jpg","B.jpg","C.jpg","D.jpg"};
        createSeqFile(names, "/seq.file");
    }

    private static void writeToHDFS(String localPath, String hdfsPath) throws IOException {
        //FileInputStream fis = new FileInputStream(localPath);
        InputStream his_is = AccessHDFS.class.getClass().getResourceAsStream("/"+localPath);
        Path out = new Path(hdfsPath);
        OutputStream os = fs.create(out);
        org.apache.hadoop.io.IOUtils.copyBytes(his_is, os, conf);
    }

    private static void readFromHDFS(String hdfsPath, String localPath) throws IOException {
        FileOutputStream fos = new FileOutputStream(localPath);
        Path pt=new Path(hdfsPath);
        InputStream is = fs.open(pt);
        org.apache.hadoop.io.IOUtils.copyBytes(is, fos, conf);
    }

    private static void createSeqFile(String[] names,String outpath) throws IOException {

        Path path = new Path(outpath);
        InputStream his_is;
        SequenceFile.Writer writer =
                SequenceFile.createWriter(path.getFileSystem(conf), conf, path,
                        new Text().getClass(), new BytesWritable().getClass());

        for (int i = 0; i< names.length;i++ ){
            his_is = AccessHDFS.class.getClass().getResourceAsStream("/"+names[i]);
            writer.append(new Text(names[i]),new BytesWritable(IOUtils.toByteArray(his_is)));
        }

    }
}
