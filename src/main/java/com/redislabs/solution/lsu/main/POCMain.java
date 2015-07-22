
package com.redislabs.solution.lsu.main;
import com.google.common.base.Stopwatch;
import com.redislabs.solution.lsu.JedisClient;
import com.redislabs.solution.lsu.objects.POCValue;
import com.redislabs.solution.lsu.util.POCUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * POCMain
 */

public class POCMain {

    private final int NUMBER_OF_THREADS = 50;
    final private JedisClient jedisClient;



    public POCMain(String host, int port){

        //init jedis
        jedisClient = new JedisClient(host,port);

    }

    public static void main(String[] args) throws Exception {

//        if ( args == null || args.length == 3 )
//            throw new Exception("Usage: host port data-directory crcsize");

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String directory = args[2];
        int crcsize = Integer.parseInt(args[3]);

        POCMain pocMain = new POCMain(host,port);

//        // debug
//        for (int i = 32; i >  0; i--)
//            System.out.println("i: " + i + ", crc32': " + POCUtil.hashKey("ABCDE", i).getBytes());

        try {
            pocMain.run(directory, crcsize);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    /**
     * this code will be based on the line format and will not be generic
     */

    public void run(String directory, final int crcsize) throws Exception {




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
        List<File> files =  new ArrayList<>(Arrays.asList(listOfFiles));

        /*
        split the files in the directory into 10 lists
         */
        final List<List<File>> partitionFiles =   POCUtil.chopIntoParts(files,50);  //Lists.partition(files, NUMBER_OF_THREADS);

        Thread[] readThreads = new Thread[NUMBER_OF_THREADS];

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

                        /*
                        process current files
                         */
                        try (BufferedReader br = new BufferedReader(new FileReader(currentFile))) {

                            /*
                            line optimization using pipeline
                             */
                            List<String> lines = new ArrayList<>();
                            String line;
                            while ((line = br.readLine()) != null) {
                                // process the line.

                                lines.add(line);

                                if ( lines.size() == 20 )
                                {

                                    setHashs(lines, line, crcsize);
                                    //jedisClient.hset(POCUtil.hashKey(key1), POCUtil.encodeKey(key1), line);

                                    // clear list
                                    lines.clear();

                                }
                                // do the rest of the items
                                if ( lines.size() > 0 )
                                {
                                    setHashs(lines,"",crcsize);
                                }
                            }

                            /*
                            increase file counter by 1 and and print every 100 files
                             */
                            if ((fileCounter.incrementAndGet() % 100) == 0)
                                System.out.println("Number of current files " + fileCounter.get());

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                    }
                    
                    
                    System.out.println(totalStopwatch.elapsed(TimeUnit.MILLISECONDS));
                }

                private void setHashs(List<String> lines, String line, int crcsize) {
                    List<String> keys = new ArrayList<>();
                    List<String> hashKeys = new ArrayList<>();
                    List<String> hashValues = new ArrayList<>();
                    for (int k = 0; k < lines.size(); k++) {

                        String[] array =  lines.get(k).split("\t");
                        //location of key
                        keys.add(POCUtil.hashKey(array[0], crcsize));
                        hashKeys.add(POCUtil.encodeKey(array[0]));


                        String freq = array[1].replace("Freq=", "");
                        String ie = array[2].replace("IE=","");
                        String oe = array[3].replace("OE=","");

                        if ( freq == null || ie == null || oe   == null )
                            System.out.print(line);
                        else
                        {
                            POCValue pocValueWrite = new POCValue(Integer.parseInt(freq), ie, oe);
                            hashValues.add(pocValueWrite.getRecord());
                        }

                        jedisClient.hset(keys,hashKeys,hashValues);
                    }
                }

            });
            readThreads[i].start();

        }


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
