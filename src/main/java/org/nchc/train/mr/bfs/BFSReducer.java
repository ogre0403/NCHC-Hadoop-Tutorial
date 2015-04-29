package org.nchc.train.mr.bfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by 1403035 on 2015/4/28.
 */
public class BFSReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
    private static Logger LOG = Logger.getLogger(BFSReducer.class);
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        LOG.info("Reduce executing for input key [" + key.toString() + "]");

        List<Integer> edges = null;
        int distance = Integer.MAX_VALUE;
        Node.Color color = Node.Color.WHITE;


        for (Text val : values) {

            Node u = new Node(key.get() + "\t" + val.toString());
            // One (and only one) copy of the node will be the fully expanded
            // version, which includes the edges
            if (u.getEdges().size() > 0) {
                edges = u.getEdges();
            }

            // Save the minimum distance
            if (u.getDistance() < distance) {
                distance = u.getDistance();
            }

            // Save the darkest color
            if (u.getColor().ordinal() > color.ordinal()) {
                color = u.getColor();
            }

        }

        Node n = new Node(key.get());
        n.setDistance(distance);
        n.setEdges(edges);
        n.setColor(color);
        context.write(key,new Text(n.getLine()));
        LOG.info("Reduce outputting final key [" + key + "] and value [" + n.getLine() + "]");
    }
}
