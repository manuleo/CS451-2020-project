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
                lsn += 1;
                // The message carries only the sequence number
                //System.out.println("Message to send: " + message);
                //System.out.println("Sequence number: " + lsn);
                assert (Objects.equals(message, String.valueOf(lsn)));
                try {
                    messageToSendDown.put(String.valueOf(lsn));
                } catch (InterruptedException e) {
                    System.out.println("Sending message in FIFO error: " + e.toString());
                }
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
                String gotPack = null;
                try {
                    gotPack = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in FIFO: " + e.toString());
                }
                //System.out.println(gotPack);
                String[] gotSplit = gotPack.split(" ");
                int pid = Integer.parseInt(gotSplit[0]);
                HashSet<String> messages = pending.getOrDefault(pid, new HashSet<>());
                messages.add(gotPack);
                pending.put(pid, messages);
                while (true) {
                    HashSet<String> newMess = pending.get(pid);
                    String toDeliver = newMess.stream()
                            .filter(x -> Integer.parseInt(x.split(" ")[1]) == seqNums[pid-1])
                            .findFirst()
                            .orElse("NOMESS");
                    //System.out.println(toDeliver);
                    if(toDeliver.equals("NOMESS"))
                        break;
                    else {
                        seqNums[pid-1]+=1;
                        newMess.remove(toDeliver);
                        pending.put(pid, newMess);
                        try {
                            messageToDeliverUp.put(toDeliver);
                        } catch (InterruptedException e) {
                            System.out.println("Unable to deliver packet in FIFO: " + e.toString());
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
