package org.nchc.train.mr.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/**
 * Created by 1403035 on 2014/7/16.
 */
public class ImageProcess {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Read Image");
        job.setJarByClass(ImageProcess.class);
        job.setMapperClass(ImageMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
