package org.nchc.bigdata.mapreduce.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SqeFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    private static FileSystem fs;
    private static Configuration conf;

    @Override
     protected void setup(Context context) throws IOException, InterruptedException {
        fs = FileSystem.get(context.getConfiguration());
        conf = context.getConfiguration();
    }

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
//        String fname = key.toString();
        String[] sa = key.toString().split("/");
        String fname = sa[sa.length-1];
        int sss = value.getLength();
        byte[] ba = new byte[sss];
        System.arraycopy(value.getBytes(),0,ba,0,sss);
        BufferedImage src = ImageIO.read(new ByteArrayInputStream(ba));

        int w = src.getWidth();
        int h = src.getHeight();
        for (int x = 0; x < w; x++) {
            for (int y = 0; y < h; y++) {
                int rgb = src.getRGB(x, y);
                int a = (rgb & 0xFF000000) >>> 24;
                int r = (rgb & 0x00FF0000) >>> 16;
                int g = (rgb & 0x0000FF00) >>> 8;
                int b = (rgb & 0x000000FF) >>> 0;
                if (r > 128) {
                    int new_alpha = 255;
                    int new_red = 255;
                    int new_green = 255;
                    int new_blue = 255;

                    int new_rgb = (new_alpha << 24) | (new_red << 16) | (new_green << 8) | new_blue;
                    src.setRGB(x, y, new_rgb);
                } else {
                    int new_alpha = 0;
                    int new_red = 0;
                    int new_green = 0;
                    int new_blue = 0;

                    int new_rgb = (new_alpha << 24) | (new_red << 16) | (new_green << 8) | new_blue;
                    src.setRGB(x, y, new_rgb);
                }
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write( src, "jpg", baos );
        baos.flush();
        byte[] imageInByte = baos.toByteArray();
        baos.close();

        context.write(new Text(fname),new BytesWritable(imageInByte));
    }


    private void binarization(Text key, BytesWritable value) throws IOException {
        String fname = key.toString();
        int sss = value.getLength();
        byte[] ba = new byte[sss];
        System.arraycopy(value.getBytes(),0,ba,0,sss);
        BufferedImage src = ImageIO.read(new ByteArrayInputStream(ba));

        int w = src.getWidth();
        int h = src.getHeight();
        for (int x = 0; x < w; x++) {
            for (int y = 0; y < h; y++) {
                int rgb = src.getRGB(x, y);
                int a = (rgb & 0xFF000000) >>> 24;
                int r = (rgb & 0x00FF0000) >>> 16;
                int g = (rgb & 0x0000FF00) >>> 8;
                int b = (rgb & 0x000000FF) >>> 0;
                if (r > 128) {
                    int new_alpha = 255;
                    int new_red = 255;
                    int new_green = 255;
                    int new_blue = 255;

                    int new_rgb = (new_alpha << 24) | (new_red << 16) | (new_green << 8) | new_blue;
                    src.setRGB(x, y, new_rgb);
                } else {
                    int new_alpha = 0;
                    int new_red = 0;
                    int new_green = 0;
                    int new_blue = 0;

                    int new_rgb = (new_alpha << 24) | (new_red << 16) | (new_green << 8) | new_blue;
                    src.setRGB(x, y, new_rgb);
                }
            }
        }

        Path out = new Path("/out/"+fname);
        OutputStream os = fs.create(out);
        ImageIO.write(src, "jpg", os);
        os.close();
        //ByteArrayInputStream is = new ByteArrayInputStream(ba);
        //org.apache.hadoop.io.IOUtils.copyBytes(is, os, conf);
    }
}
