package cs451;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class FIFO {
    private int id;
    private UniformReliableBroadcast urb;
    private int lsn = 0;
    private HashMap<Integer, HashSet<String>> pending = new HashMap<>();
    private int[] seqNums;
    private LinkedBlockingQueue<String> messageToSendDown;
    private LinkedBlockingQueue<String> messageToSendUp;
    private LinkedBlockingQueue<String> messageDeliveredDown;
    private LinkedBlockingQueue<String> messageToDeliverUp;

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
            while(true) {
                String message = null;
                try {
                    message = messageToSendUp.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting message in FIFO error: " + e.toString());
                }
                List<String> messages = new LinkedList<>();
                messages.add(message);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    System.out.println("Sleeping in FIFO error: " + e.toString());
                }
                messageToSendUp.drainTo(messages);
                lsn += messages.size();
                // The message carries only the sequence number
                //System.out.println("Message to send: " + message);
                //System.out.println("Sequence number: " + lsn);
                //System.out.println("I'm sending" + messages);
                messageToSendDown.addAll(messages);
            }
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
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                     System.out.println("Sleeping in FIFO deliver: " + e.toString());
                }
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
