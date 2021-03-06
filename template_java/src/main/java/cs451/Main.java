package cs451;

import javax.sound.midi.Receiver;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static LinkedBlockingQueue<String> messageToSend = new LinkedBlockingQueue<>();
    private static LinkedBlockingQueue<String> messageDelivered = new LinkedBlockingQueue<>();
    protected static HashSet<String> broadcasted = new HashSet<>();
    protected static Coordinator coordinator;
    protected static int m;
    private static LinkedList<String> recPack = new LinkedList<>();
    protected static LinkedList<String> out = new LinkedList<>();
    private static String outName;
    protected static final Object lockOut = new Object();

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
        //System.out.println(recPack);
        System.out.println("Total message delivered: " + recPack.size());

        try (FileWriter fw = new FileWriter(outName)) {
            synchronized (lockOut) {
                for (String s: out) {
                    fw.write(s + "\n");
                }
            }
        } catch (IOException e) {
            System.out.println("Impossible to write " + e.toString());
        }

        //System.out.println(UniformReliableBroadcast.ack);

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

        // example
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
        outName = parser.output();
        m = 0;
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
            m = parseConfig(parser.config());
        }
        coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());
        System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

        System.out.println("Broadcasting messages...");
        FIFOBroadcast(parser);
        //testUniformReliableBroadcast(parser);

//        System.out.println("Signaling end of broadcasting messages");
//        coordinator.finishedBroadcasting();

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    private static int parseConfig(String config) {
        Scanner input = null;
        try {
            input = new Scanner(new File(config));
        } catch (FileNotFoundException e) {
            System.out.println("File config not found exception! " + e.toString());
        }
        int i = 0;
        int m  = 0;
        while(input.hasNextLine()) {
            String data = input.nextLine();
            if (i==0) {
                m = Integer.parseInt(data);
            }
            i+=1;
        }
        input.close();
        return m;
    }

//    private static void testPerfectLink(Parser parser) throws UnknownHostException {
//        if (parser.myId()==1) {
//            PerfectLink pf1 = new PerfectLink(parser.myId(), parser.hosts().get(0).getPort());
//            pf1.receiveAndDeliver();
//            for (int i=0; i<100; i++) {
//                pf1.send(i, InetAddress.getByName(parser.hosts().get(1).getIp()),
//                        parser.hosts().get(1).getPort());
//            }
//        }
//        else {
//            PerfectLink pf2 = new PerfectLink(parser.myId(), parser.hosts().get(1).getPort());
//            pf2.receiveAndDeliver();
//            for (int i=0; i<100; i++) {
//                pf2.send(i, InetAddress.getByName(parser.hosts().get(0).getIp()),
//                        parser.hosts().get(0).getPort());
//            }
//        }

//    private static void testBestEffortBroadcast(Parser parser) {
//        BestEffortBroadcast beb = new BestEffortBroadcast(parser.hosts(), parser.myId());
//        class TestDeliver extends Thread {
//            @Override
//            public void run() {
//                while (true) {
//                    List<String> gotPacks = beb.receiveAndDeliver();
//                    recPack.addAll(gotPacks);
//                }
//            }
//        }
//        new TestDeliver().start();
//        for (int i = 0; i<25; i++)
//            beb.broadcast(String.valueOf(i));
//    }

    private static void testUniformReliableBroadcast(Parser parser) throws InterruptedException {
        UniformReliableBroadcast urb = new UniformReliableBroadcast(parser.hosts(), parser.myId(), messageToSend, messageDelivered);
        class Deliver extends Thread {
            @Override
            public void run() {
                while (true) {
                    String gotPack = null;
                    try {
                        gotPack = messageDelivered.take();
                        //System.out.println("Got: " + gotPack);
                    } catch (InterruptedException e) {
                        System.out.println("Getting message in main error: " + e.toString());
                    }
                    List<String> newGot = new LinkedList<>();
                    newGot.add(gotPack);
//                    synchronized (lockOut) {
//                        out.add("d " + gotPack);
//                    }
//                    recPack.add(gotPack);
//                    try {
//                        Thread.sleep(0);
//                    } catch (InterruptedException e) {
//                        System.out.println("Sleeping in main error: " + e.toString());
//                    }
                    messageDelivered.drainTo(newGot);
                    //System.out.println("Received in main: " + newGssot);
                    synchronized (lockOut) {
                        for (String got: newGot)
                            out.add("d " + got);
                    }
                    recPack.addAll(newGot);
                    if (recPack.size()%100==0)
                        System.out.println("Delivered " + recPack.size() + " packets");
                    if (recPack.size() == parser.hosts().size() * m) {
                        System.out.println("Received everything from everyone.");
                        return;
                    }
                }
            }
        }
//        class Send extends Thread {
//            final int m;
//            public Send(int m) {
//                this.m = m;
//            }
//            @Override
//            public void run() {
//                for (int i = 1; i<=m; i++) {
//                    try {
//                        messageToSend.put(String.valueOf(i));
//                    } catch (InterruptedException e) {
//                        System.out.println("Sending message in main error: " + e.toString());
//                    }
//                    synchronized (lockOut) {
//                        out.add("b " + i);
//                    }
//                    broadcasted.add(parser.myId() + " " + i);
//                }
//            }
//        }
        Deliver deliver = new Deliver();
//        Send send = new Send(m);
        deliver.start();
//        send.start();
        try {
//            send.join();
            deliver.join();
        } catch (InterruptedException e) {
            System.out.println("Error while waiting in main: " + e.toString());
        }
    }

//    private static void testFIFO(Parser parser) throws InterruptedException {
//        LinkedBlockingQueue<String> messageToSend = new LinkedBlockingQueue<>();
//        LinkedBlockingQueue<String> messageDelivered = new LinkedBlockingQueue<>();
//        FIFO fifo = new FIFO(parser.hosts(), parser.myId(), messageToSend, messageDelivered);
//        class TestDeliver extends Thread {
//            @Override
//            public void run() {
//                while (true) {
//                    String gotPack = null;
//                    try {
//                        gotPack = messageDelivered.take();
//                    } catch (InterruptedException e) {
//                        System.out.println("Getting message in main error: " + e.toString());
//                    }
//                    out.add("d " + gotPack);
//                    recPack.add(gotPack);
//                    if (recPack.size()%100==0)
//                        System.out.println(recPack.size());
//                }
//            }
//        }
//        new TestDeliver().start();
//        for (int i = 1; i<=100; i++) {
//            messageToSend.put(String.valueOf(i));
//            out.add("b " + i);
//        }
//    }

    private static void FIFOBroadcast(Parser parser) {
        FIFO fifo = new FIFO(parser.hosts(), parser.myId(), messageToSend, messageDelivered);
        class Deliver extends Thread {
            @Override
            public void run() {
                while (true) {
                    String gotPack = null;
                    try {
                        gotPack = messageDelivered.take();
                        //System.out.println("Got: " + gotPack);
                    } catch (InterruptedException e) {
                        System.out.println("Getting message in main error: " + e.toString());
                    }
                    List<String> newGot = new LinkedList<>();
                    newGot.add(gotPack);
//                    synchronized (lockOut) {
//                        out.add("d " + gotPack);
//                    }
//                    recPack.add(gotPack);
//                    try {
//                        Thread.sleep(0);
//                    } catch (InterruptedException e) {
//                        System.out.println("Sleeping in main error: " + e.toString());
//                    }
                    messageDelivered.drainTo(newGot);
                    //System.out.println("Received in main: " + newGssot);
                    synchronized (lockOut) {
                        for (String got: newGot)
                            out.add("d " + got);
                    }
                    recPack.addAll(newGot);
                    if (recPack.size()%100==0)
                        System.out.println("Delivered " + recPack.size() + " packets");
                    if (recPack.size() == parser.hosts().size() * m) {
                        System.out.println("Received everything from everyone.");
                        return;
                    }
                }
            }
        }
//        class Send extends Thread {
//            final int m;
//            public Send(int m) {
//                this.m = m;
//            }
//            @Override
//            public void run() {
//                for (int i = 1; i<=m; i++) {
//                    try {
//                        messageToSend.put(String.valueOf(i));
//                    } catch (InterruptedException e) {
//                        System.out.println("Sending message in main error: " + e.toString());
//                    }
//                    synchronized (lockOut) {
//                        out.add("b " + i);
//                    }
//                    broadcasted.add(parser.myId() + " " + i);
//                }
//            }
//        }
        Deliver deliver = new Deliver();
//        Send send = new Send(m);
        deliver.start();
//        send.start();
        try {
//            send.join();
            deliver.join();
        } catch (InterruptedException e) {
            System.out.println("Error while waiting in main: " + e.toString());
        }
    }

}
