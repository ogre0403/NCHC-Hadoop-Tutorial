package org.nchc.bigdata.mapreduce.LOG.generator;


import org.apache.log4j.Logger;

public class LogGenerator {

    private static Logger logger = Logger.getLogger(LogGenerator.class);
    public static void main(String[] args) {

        int a;
        long num;
        if (args.length == 2) {
            a = Integer.parseInt(args[0]);
            num = Long.parseLong(args[1]);
        }else{
            a = 3;
            num =1;
        }
        logger.info("a = " + a + "\n");
        logger.info("num = " + num + "\n");
        Thread[] threads = new Thread[3*a];

        for(int i = 0;i<a;i++){
            threads[i] = new Thread(new LogThread1(num));
        }

        for(int i = a;i<2*a;i++){
            threads[i] = new Thread(new LogThread2(num));
        }

        for(int i = 2*a;i<3*a;i++){
            threads[i] = new Thread(new LogThread3(num));
        }

        for(int i = 0;i<3*a;i++){
            threads[i].start();
        }


    }


}
