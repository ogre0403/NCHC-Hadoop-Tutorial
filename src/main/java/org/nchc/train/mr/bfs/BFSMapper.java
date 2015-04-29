package org.nchc.train.mr.bfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by 1403035 on 2015/4/28.
 */
public class BFSMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static Logger LOG = Logger.getLogger(BFSMapper.class);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {

        LOG.info("Map executing for key [" + key.toString() + "] and value [" + value.toString() + "]");

        Node node = new Node(value.toString());

        // For each GRAY node, emit each of the edges as a new node (also GRAY)
        if (node.getColor() == Node.Color.GRAY) {
            for (int v : node.getEdges()) {
                Node vnode = new Node(v);
                vnode.setDistance(node.getDistance() + 1);
                vnode.setColor(Node.Color.GRAY);
                context.write(new IntWritable(vnode.getId()), vnode.getLine());
            }
            // We're done with this node now, color it BLACK
            node.setColor(Node.Color.BLACK);
        }

        // No matter what, we emit the input node
        // If the node came into this method GRAY, it will be output as BLACK
        context.write(new IntWritable(node.getId()), node.getLine());

        LOG.info("Map outputting for key[" + node.getId() + "] and value [" + node.getLine() + "]");
    }
}
