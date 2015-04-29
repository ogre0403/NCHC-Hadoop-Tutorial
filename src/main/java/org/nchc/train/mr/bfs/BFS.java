package org.nchc.train.mr.bfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by 1403035 on 2015/4/28.
 */
public class BFS extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BFS(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        int iterationCount = 0;

        while (keepGoing(iterationCount)) {

            String input;
            if (iterationCount == 0)
                input = "input-graph";
            else
                input = "output-graph-" + iterationCount+"/part-r-00000";

            String output = "output-graph-" + (iterationCount + 1);

            Job job = Job.getInstance(conf, "BFS-" + (iterationCount + 1));
            job.setJarByClass(BFS.class);
            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);

            iterationCount++;
        }

        return 0;
    }


    private boolean keepGoing(int iterationCount) {
        if(iterationCount >= 8) {
            return false;
        }
        return true;
    }
}
