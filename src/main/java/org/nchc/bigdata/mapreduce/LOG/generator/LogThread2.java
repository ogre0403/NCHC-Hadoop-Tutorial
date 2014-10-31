package org.nchc.bigdata.mapreduce.LOG.generator;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;

/**
 * Created by 1403035 on 2014/10/6.
 */
public class LogThread2 implements Runnable{

    private static Logger logger = Logger.getLogger(LogThread2.class);
    private long num;
    private Random rr = new Random(System.currentTimeMillis());

    public LogThread2(long _num){
        num= _num;
    }

    public void run(){

        for(int i = 0;i<num;i++){
            try{
                ExceptionGen(rr.nextInt(10));
            }catch(Throwable t){
                StringWriter errors = new StringWriter();
                t.printStackTrace(new PrintWriter(errors));
                logger.error(errors.toString());
            }
        }
    }

    private void ExceptionGen(int r) throws Throwable{
        switch(r) {
            case 0:
                throw new OutOfMemoryError();
            case 1:
                throw new ArrayIndexOutOfBoundsException();
            case 2:
                throw new FileNotFoundException();
            case 3:
                throw new NullPointerException();
            case 4:
                throw new ClassNotFoundException();
            default:
                logger.info("NORMAL\n");
        }
    }
}
