package org.nchc.bigdata.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public  class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static Logger logger = Logger.getLogger(WordCountMapper.class);
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      logger.info("input sentence " + value.toString());
      StringTokenizer itr = new StringTokenizer(value.toString());
      int i = 0;
      while (itr.hasMoreTokens()) {
        i++;
        String _word;
        _word = itr.nextToken();
        logger.info( i + "_th world in input sentence is [" + _word + "]");
        word.set(_word);
        context.write(word, one);
      }
    }
  }
