package org.nchc.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by 1403035 on 2014/8/6.
 */
public class AccessHDFS {
    private static FileSystem fs;
    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        fs = FileSystem.get(conf);
        writeToHDFS("/D:/kafka_image/A.jpg", "/AAA.JPG");
        readFromHDFS("/AAA.JPG","/D:/aaa.jpg");
    }

    private static void writeToHDFS(String localPath, String hdfsPath) throws IOException {
        FileInputStream fis = new FileInputStream(localPath);
        Path out = new Path(hdfsPath);
        OutputStream os = fs.create(out);
        org.apache.hadoop.io.IOUtils.copyBytes(fis, os, conf);
    }

    private static void readFromHDFS(String hdfsPath, String localPath) throws IOException {
        FileOutputStream fos = new FileOutputStream(localPath);
        Path pt=new Path(hdfsPath);
        InputStream is = fs.open(pt);
        org.apache.hadoop.io.IOUtils.copyBytes(is, fos, conf);
    }
}
