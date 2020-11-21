package cs451;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static String outName;
    private static Coordinator coordinator;
    private static int m;
    private static HashSet<Integer> influences = new HashSet<>();
    protected static LinkedList<String> out = new LinkedList<>();
    private static final LinkedBlockingQueue<String> messageDelivered = new LinkedBlockingQueue<>();
    private static final LinkedList<String> recPack = new LinkedList<>();
    protected static final Object lockOut = new Object();

    private static void handleSignal() {

        // Log everything it was delivered
        System.out.println("Immediately stopping network packet processing.");
        System.out.println("Writing output.");
        System.out.println("Total message delivered: " + recPack.size());

        try (FileWriter fw = new FileWriter(outName)) {
            // Avoid concurrent writing of packets in delivering
            synchronized (lockOut) {
                for (String s: out) {
                    fw.write(s + "\n");
                }
            }
        } catch (IOException e) {
            System.out.println("Impossible to write " + e.toString());
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        long pid = ProcessHandle.current().pid();
        System.out.println("My PID is " + pid + ".");
        System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");

        System.out.println("My id is " + parser.myId() + ".");
        System.out.println("List of hosts is:");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
        }

        System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
        System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
        System.out.println("Output: " + parser.output());

        // Load number of messages to broadcast
        outName = parser.output();
        m = 0;
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
            parseConfig(parser.config(), parser.myId());
//            // By some analysis I discovered this configuration as being better when m<=10000
//            if (m<=10000) {
//                Constants.WINDOW_SIZE = 500;
//                Constants.INIT_THRESH = 2000;
//            }
        }
        // Set up coordinator
        coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());
        System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

        // Start broadcast
        System.out.println("Broadcasting messages...");
        LCausalBroadcast(parser);
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    /**
     * Get number of messages to deliver from configuration file
     * @param config: configuration file path
     * @return number of messages to broadcast, processes influencing the current process
     */
    private static void parseConfig(String config, int id) {
        Scanner input = null;
        try {
            input = new Scanner(new File(config));
        } catch (FileNotFoundException e) {
            System.out.println("File config not found exception! " + e.toString());
        }
        int i = 0;
        while(input.hasNextLine()) {
            // Read first line: integer m
            String data = input.nextLine();
            if (i==0) {
                m = Integer.parseInt(data);
            } else {
                String[] splits = data.split(" ");
                if (Integer.parseInt(splits[0]) == id && splits.length > 1) {
                    for (int j = 1; j < splits.length; j++)
                        influences.add(Integer.parseInt(splits[j]));
                    break;
                }
            }
            i+=1;
        }
        input.close();
        //influences.add(id);
    }


    /**
     * Broadcast LCausal messages
     * @param parser the argument parser
     */
    private static void LCausalBroadcast(Parser parser) {
        // Start the LCausal
        LCausal lCausal = new LCausal(parser.hosts(), parser.myId(), messageDelivered, coordinator, m, influences);

        /**
        * Deliver class to get messages delivered from low levels
        */
        class Deliver extends Thread {
            @Override
            public void run() {
                while (true) {
                    // Wait until a package is ready to be delivered
                    String gotPack = null;
                    try {
                        gotPack = messageDelivered.take();
                    } catch (InterruptedException e) {
                        System.out.println("Getting message in main error: " + e.toString());
                    }
                    // Get everything in the queue to deliver a batch
                    List<String> newGot = new LinkedList<>();
                    newGot.add(gotPack);
                    messageDelivered.drainTo(newGot);
                    // Add the messages as delivered
                    synchronized (lockOut) {
                        for (String got: newGot)
                            out.add("d " + got);
                    }
                    // Add the messages as received to print some control messages
                    recPack.addAll(newGot);
                    if (recPack.size()%100==0)
                        System.out.println("Delivered " + recPack.size() + " packets");
                }
            }
        }

        // Start delivering messages and attach this thread to it (will wait forever)
        Deliver deliver = new Deliver();
        deliver.start();
        try {
            deliver.join();
        } catch (InterruptedException e) {
            System.out.println("Error while waiting in main: " + e.toString());
        }
    }

}
