package cs451;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    private static Set<String> recPack = new HashSet<>();

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
//        System.out.println(recPack);
        System.out.println(recPack.size());
//        Set<String> testPack = new HashSet<>();
//        for(int h = 1; h<4; h++) {
//            for (int m = 0; m < 50; m++) {
//                String t = String.format("%d %d", h,m);
//                testPack.add(t);
//            }
//        }
//        testPack.removeAll(recPack);
//        System.out.println("Missing messages " + testPack);
//        System.out.println(PerfectLink.getSentMessage());
//        for (Map.Entry<Integer, HashSet<String>> entry : PerfectLink.getRecACKs().entrySet()) {
//                    System.out.println(entry.getKey() + "=" + entry.getValue());
//        }
//        System.out.println(PerfectLink.getRecMessage());
//        System.out.println(PerfectLink.getGotRec());
//        System.out.println(PerfectLink.getGotSend());
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
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
        }


        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

        System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

        System.out.println("Broadcasting messages...");
        testUniformReliableBroadcast(parser);

        System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
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

    private static void testBestEffortBroadcast(Parser parser) {
        BestEffortBroadcast beb = new BestEffortBroadcast(parser.hosts(), parser.myId());
        class TestDeliver extends Thread {
            @Override
            public void run() {
                while (true) {
                    List<String> gotPacks = beb.receiveAndDeliver();
                    recPack.addAll(gotPacks);
                }
            }
        }
        new TestDeliver().start();
        for (int i = 0; i<25; i++)
            beb.broadcast(String.valueOf(i));
    }

    private static void testUniformReliableBroadcast(Parser parser) {
        UniformReliableBroadcast urb = new UniformReliableBroadcast(parser.hosts(), parser.myId());
        class TestDeliver extends Thread {
            @Override
            public void run() {
                while (true) {
                    List<String> gotPacks = urb.receiveAndDeliver();
                    recPack.addAll(gotPacks);
                }
            }
        }
        new TestDeliver().start();
        for (int i = 0; i<1000; i++)
            urb.broadcast(String.valueOf(i));
    }
}
