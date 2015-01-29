package org.nchc.bigdata.mapreduce.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;

/**
 * Created by 1403035 on 2014/7/16.
 */
public class SeqFileExample {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Read Image");
        job.setJarByClass(SeqFileExample.class);
        job.setMapperClass(SqeFileMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(8);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        /*
        if(job.waitForCompletion(true)) {
            Path path = new Path("/out/part-r-00000");
            SequenceFile.Reader reader =
                    new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

            Text key = new Text();
            BytesWritable val = new BytesWritable();
            while (reader.next(key, val)) {
                System.out.println(key);
                int sss = val.getLength();
                byte[] ba = new byte[sss];
                System.arraycopy(val.getBytes(),0,ba,0,sss);
                BufferedImage src = ImageIO.read(new ByteArrayInputStream(ba));

                FileOutputStream fos = new FileOutputStream("/home/ogre/TestImage/"+key);
                ImageIO.write(src, "jpg", fos);
                fos.close();
            }
        }
        */

    }
}
