
package com.redislabs.solution.lsu.main;
import com.google.common.base.Stopwatch;
import com.redislabs.solution.lsu.objects.JedisClient;
import com.redislabs.solution.lsu.objects.POCValue;
import com.redislabs.solution.lsu.util.POCUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * POCLoad
 */

public class POCLoad {

    /*
    input params
     */
    private static String host;
    private static int port;
    private static long mod;
    private static int numOfThreads;
    private static boolean readFiles;
    private static int pipeLineSize;
    private static String directory;


    final private JedisClient jedisClient;


    public POCLoad(String host, int port){

        //init jedis
        jedisClient = new JedisClient(host,port);

    }

    public static void main(String[] args) throws Exception {


        // todo: better command line argument parsing
        validateAndSetArguments(args);

        POCLoad POCLoad = new POCLoad(host,port);

        try {
            POCLoad.run(directory, mod, numOfThreads,readFiles,pipeLineSize);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void validateAndSetArguments(String[] args) {

        //        if ( args == null || args.length == 3 )
//            throw new Exception("Usage: host port data-directory mod threads");


        host = args[0];
        port = Integer.parseInt(args[1]);
        directory = args[2];
        mod = Long.parseLong(args[3]);
        numOfThreads = Integer.parseInt(args[4]);
        readFiles = false;
        if (args[5].toString().equalsIgnoreCase("true"))
            readFiles = true;
        pipeLineSize=Integer.parseInt(args[6]);

        System.out.println("input parameters ");
        System.out.println("------------------------------------------");
        System.out.println(" host name    " + host);
        System.out.println(" port         " + port);
        System.out.println(" directory    " + directory);
        System.out.println(" numOfThreads " + numOfThreads);
        System.out.println(" read only    " + readFiles);
        System.out.println(" pipeline     " + pipeLineSize);


    }

    /**
     * this code will be based on the line format and will not be generic
     */

    public void run(String directory, final long mod, int nthreads, final boolean readFiles, final int pipelineSize) throws Exception {

        /*
        this method will go over all files in the directory
         */


        File folder = new File(directory);
        File[] listOfFiles = folder.listFiles();

        if ( listOfFiles == null )
            throw new Exception("files not found in directory ");

        /*
        get a list of all files in this directory
         */
        List<File> files =  Arrays.asList(listOfFiles);

        /*
        split the files in the directory into 10 lists
         */
        final List<List<File>> partitionFiles = POCUtil.chopIntoParts(files, nthreads);  //Lists.partition(files, NUMBER_OF_THREADS);

        Thread[] readThreads = new Thread[nthreads];

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger fileCounter = new AtomicInteger(0);

        for (int i = 0; i < readThreads.length; i++) {

            readThreads[i] = new Thread(new Runnable() {

                public void run() {

                    Stopwatch totalStopwatch = Stopwatch.createStarted();
                    int currentItem = counter.getAndIncrement();
                    /*
                    go over current files in this partition
                     */
                    for (int j = 0; j < partitionFiles.get(currentItem).size(); j++) {

                        File currentFile =  partitionFiles.get(currentItem).get(j);
                        System.out.println("[" + currentItem + "] reading: " + currentFile.getAbsoluteFile());
                        /*
                        process current files
                         */
                        try {

                            /*
                            line optimization using pipeline
                             */
                            List<String> lines = new ArrayList<>();
                            String line;
                            BufferedReader br = new BufferedReader(new FileReader(currentFile));
                            while ((line = br.readLine()) != null) {
                                // process the line.
                                lines.add(line);

                                if ( lines.size() == pipelineSize )
                                {
                                    if ( readFiles )
                                        getHashs(lines, mod);
                                    else
                                       setHashs(lines, mod);

                                    // clear list
                                    lines.clear();

                                }

                            }
                            br.close();

                            // do the rest of the items
                            if ( lines.size() > 0 )
                            {
                                /*
                                this try and catch will make sure that if there is a faulty line we will still continue continue
                                 */
                                try {
                                    if ( readFiles)
                                        getHashs(lines, mod);
                                    else
                                        setHashs(lines, mod);

                                } catch (Exception e) {
                                    System.out.println("issue with this line " + line);
                                }
                                lines.clear();
                            }

                            /*
                            increase file counter by 1 and and print every 100 files
                             */
                            System.out.println("[" + currentItem + "] total of " + fileCounter.incrementAndGet() + " files processed by all threads");

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                    }
                    
                    
                    System.out.println(totalStopwatch.elapsed(TimeUnit.MILLISECONDS));
                }

                private String[] parseLine(String l) {
                    String r[] = l.split("\t");
                    assert r.length == 4;

                    r[1] = r[1].replace("Freq=", "");
                    r[2] = r[2].replace("IE=","");
                    r[3] = r[3].replace("OE=", "");

                    return(r);
                }

                private void setHashs(List<String> lines, long mod) {
                    List<byte[]> keys = new ArrayList<>();
                    List<byte[]> hashKeys = new ArrayList<>();
                    List<byte[]> hashValues = new ArrayList<>();

                    for (int k = 0; k < lines.size(); k++) {

                        String[] line =  parseLine(lines.get(k));

                        //location of key
                        keys.add(POCUtil.hashKey(line[0], mod));
                        hashKeys.add(POCUtil.encodeKey(line[0]));

                        {
                            POCValue pocValueWrite = new POCValue(Integer.parseInt(line[1]), line[2], line[3]);
                            hashValues.add(pocValueWrite.getRecord());
                        }
                    }
                    jedisClient.hset(keys, hashKeys, hashValues);
                }
                private void getHashs(List<String> lines, long mod) {

                    List<byte[]> keys = new ArrayList<>();
                    List<byte[]> hashKeys = new ArrayList<>();

                    String[] line = new String[0];

                    for (int k = 0; k < lines.size(); k++) {

                        line = parseLine(lines.get(k));

                        keys.add(POCUtil.hashKey(line[0], mod));
                        hashKeys.add(POCUtil.encodeKey(line[0]));

                    }
                    List<Object> results  =  jedisClient.hget(keys, hashKeys);

                    for (Object res : results) {

                        byte[] resByte = (byte[]) res;

                        if (resByte == null ||  resByte.length != 7) {
                            // the following will work properly **only** if pipelining == 1
                            //System.out.println("Error reading record, invalid length: " + resByte);
                            System.out.println(line[0] + "\t" + line[1] + "\t" + line[2] + "\t" + line[3]);
                        } else {
                            POCValue pocValue = new POCValue(resByte);
                        }
                    }
                }
            });
            readThreads[i].start();

        }

        for (Thread readThread : readThreads)
            readThread.join();


//        for (int i = 0; i < listOfFiles.length; i++) {
//
//            if (listOfFiles[i].isFile()) {
//
//                System.out.println("File " + listOfFiles[i].getName());
//
//                File currentFile = listOfFiles[i];
//
//                try (BufferedReader br = new BufferedReader(new FileReader(currentFile))) {
//                    String line;
//                    while ((line = br.readLine()) != null) {
//                        // process the line.
//
//
//                        String[] array =  line.split("\t");
//                        //location of key
//                        String key1 = array[0];
//                        String freq1 = array[1].replace("Freq=","");
//                        String ie1 = array[2].replace("IE=","");
//                        String oe1 = array[3].replace("OE=","");
//
//                        jedis.hset(POCUtil.hashKey(key1), POCUtil.encodeKey(key1), line);
//
//                        System.out.println(jedis.hget(POCUtil.hashKey(key1), POCUtil.encodeKey(key1)));
//                        System.out.println(line);
//                        //if  (res.equalsIgnoreCase(line))
//                        //    System.out.print("Error getting value for line " + line);
//
//
//                        //System.out.println(key1 + "-" + freq1 + "-" + ie1 + "-" + oe1);
//
//                    }
//                } catch (FileNotFoundException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//            } else if (listOfFiles[i].isDirectory()) {
//                System.out.println("Directory " + listOfFiles[i].getName());
//            }
//        }

    }

}
