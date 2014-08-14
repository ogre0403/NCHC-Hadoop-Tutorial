package org.nchc.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by 1403035 on 2014/8/11.
 */
public class TestHBase {
    private static Configuration conf = null;

    public static void main(String[] args) throws IOException {

        conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists("ttt")) {
            System.out.println("table already exists!");
        } else {
            System.out.println("not exist");
        }

    }
}
