package cs451;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class FIFO {
    private int id;
    private UniformReliableBroadcast urb;
    private int lsn = 0;
    private HashMap<Integer, HashSet<String>> pending = new HashMap<>();
    private int[] seqNums;
    protected static int windowLimit = Constants.WINDOW_SIZE;
    private LinkedBlockingQueue<String> messageToSendDown;
    private LinkedBlockingQueue<String> messageToSendUp;
    private LinkedBlockingQueue<String> messageDeliveredDown;
    private LinkedBlockingQueue<String> messageToDeliverUp;
    protected static final Object lockSending = new Object();

    public FIFO(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToSendUp,
                LinkedBlockingQueue<String> messageToDeliverUp) {
        this.messageToSendUp = messageToSendUp;
        this.messageToDeliverUp = messageToDeliverUp;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.id = id;
        this.urb = new UniformReliableBroadcast(hosts, id, messageToSendDown, messageDeliveredDown);
        this.seqNums = new int[hosts.size()];
        Arrays.fill(this.seqNums, 1);
        receiveAndDeliver();
        broadcast();
    }

    private class Broadcast extends Thread {

        @Override
        public void run() {
            // TODO: possible optimization: move this logic to URB
            int i = 1;
            while(i<=Main.m) {
                synchronized (lockSending) {
                    while(i==windowLimit+1) {
                        try {
                            lockSending.wait();
                        } catch (InterruptedException e) {
                            System.out.println("Interrupted FIFO waiting: " + e.toString());
                        }
                    }
                    try {
                        messageToSendDown.put(String.valueOf(i));
                    } catch (InterruptedException e) {
                        System.out.println("Sending message in main error: " + e.toString());
                    }
                    synchronized (Main.lockOut) {
                        Main.out.add("b " + i);
                        //System.out.println("Broadcasted: " + Main.out);
                    }
                    Main.broadcasted.add(id + " " + i);
                    //System.out.println("Broadcasted set: " + Main.broadcasted);
                    i++;
                }
            }
            System.out.println("Signaling end of broadcasting messages");
            Main.coordinator.finishedBroadcasting();
        }
    }

    public void broadcast() {
        new Broadcast().start();
    }

    private class Receive extends Thread {

        @Override
        public void run() {
            while (true) {
                String got = null;
                try {
                    got = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in FIFO: " + e.toString());
                }
                List<String> gotPacks = new LinkedList<>();
                gotPacks.add(got);
                messageDeliveredDown.drainTo(gotPacks);
                HashSet<Integer> pids = new HashSet<>();
                for (String gotPack: gotPacks) {
                    //System.out.println("FIFO got: " + gotPack);
                    String[] gotSplit = gotPack.split(" ");
                    int pid = Integer.parseInt(gotSplit[0]);
                    pids.add(pid);
                    HashSet<String> messages = pending.getOrDefault(pid, new HashSet<>());
                    messages.add(gotPack);
                    pending.put(pid, messages);
                }
                for (int pid: pids) {
                    List<String> allDelivers = new LinkedList<>();
                    while (true) {
                        HashSet<String> newMess = pending.get(pid);
                        Optional<String> toDeliver = newMess.stream()
                                .filter(x -> Integer.parseInt(x.split(" ")[1]) == seqNums[pid-1])
                                .findFirst();
                        //System.out.println(toDeliver);
                        if(toDeliver.isEmpty()) {
                            if (allDelivers.size()!=0) {
                                newMess.removeAll(allDelivers);
                                pending.put(pid, newMess);
                                messageToDeliverUp.addAll(allDelivers);
                            }
                            break;
                        }
                        else {
                            //System.out.println("FIFO deliverable: " + toDeliver);
                            seqNums[pid-1]+=1;
                            allDelivers.add(toDeliver.get());
                        }
                    }
                }
            }
        }
    }

    public void receiveAndDeliver() {
        new Receive().start();
    }

}
