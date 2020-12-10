package cs451;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Window class representing a sliding (and congestion) window
 * which is kept by process
 */
public class Window {
    private int lowerBound; // Window lower bound
    private int upperBound; // Window upper bound
    private final ArrayList<Boolean> ackPack; // Packets to ack/already ack
    private int threshold = Constants.INIT_THRESH; // Threshold for congestion

    /**
     * Create a Window with the given size
     * @param windowSize the initial size of the window
     */
    public Window(int windowSize) {
        this.lowerBound = 1;
        this.upperBound = windowSize;
        this.ackPack = new ArrayList<>(windowSize);
        // Init the ACKs list
        for (int i = 0; i < upperBound; i++)
            ackPack.add(false);
    }

    /**
     * Check if a packet can be send (i.e. it's inside the window and it wasn't acked before)
     * @param lsn the packet lsn
     * @return true if the packet can be send, false otherwise
     */
    public boolean canSend(int lsn) {
        return lsn >= lowerBound && lsn <= upperBound && !ackPack.get(lsn - lowerBound);
    }

    /**
     * Check if packet was already ACKed
     * @param lsn the packet lsn
     * @return true if the packet was ACKed, false otherwise
     */
    public boolean alreadyAck(int lsn) {
        // If the packet is over the size of the list, it wasn't ACKed
        if (lsn >= lowerBound && lsn - lowerBound >= ackPack.size())
            return false;
        // It was ACKed if it's below the lower bound or its element in the window is to true
        return lsn < lowerBound || ackPack.get(lsn - lowerBound);
    }

    /**
     * Mark the packet as ACKed
     * @param lsn the packet lsn
     */
    public void markPacket(int lsn) {
        ackPack.set(lsn - lowerBound, true);
        // Move the window
        moveWindow();
    }

    /**
     * Move the window based on the number of ACKs received from the window start
     */
    private void moveWindow() {
        int numAck = 0;
        // Count ACK from the left size
        for (boolean mark: ackPack) {
            if (mark)
                numAck+=1;
            else
                break;
        }
        // If we have at least one ACK
        if (numAck>=0) {
            // Move window to the right
            lowerBound += numAck;
            upperBound += numAck;
            // Clear the first portion of the list and add new non-ACKed packet on the right
            ackPack.subList(0, numAck).clear();
            for (int i = 0; i < numAck; i++)
                ackPack.add(false);
        }
    }

    /**
     * Increase the size of the congestion window
     * by doubling or increasing by 1, depending on the threshold
     */
    public void increaseSize() {
        // If the window is bigger than the threshold increase by 1
        if ((upperBound - lowerBound + 1)>=threshold) {
            // If we are at the end of the list add a new packet with false
            if (upperBound - lowerBound + 1 == ackPack.size())
                ackPack.add(false);
            upperBound++; // Increase upperBound
        }
        else {
            // Double the window
            // Compute how many packets add in the end of the list
            int toAdd = Math.max(2 * (upperBound - lowerBound + 1) - ackPack.size(), 0);
            upperBound += upperBound - lowerBound + 1; // Increase upperBound to get window size doubled
            for (int i = 0; i < toAdd; i++) // Eventually add new packets
                ackPack.add(false);
        }
    }

    /**
     * Reduce window after timeout
     * Threshold = WindowSize/2
     * WindowSize = Initial window size (defined in constants, can be different from 1 unlike TCP)
     */
    public void timeoutStart() {
        threshold = Math.max((upperBound - lowerBound + 1)/2, Constants.WINDOW_SIZE);
        upperBound = lowerBound + Constants.WINDOW_SIZE - 1;
    }

    /**
     * Reduce window after dup ack
     * Threshold = WindowSize/2
     * WindowSize = threshold
     */
    public void dupAck() {
        threshold = Math.max((upperBound - lowerBound + 1)/2, Constants.WINDOW_SIZE);
        upperBound = lowerBound + threshold - 1;
    }

    /**
     * Check equality
     * @param o Another object
     * @return true/false if objects are or not the same
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return lowerBound == window.lowerBound &&
                upperBound == window.upperBound &&
                threshold == window.threshold &&
                Objects.equals(ackPack, window.ackPack);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound, ackPack, threshold);
    }

    /**
     * @return string window representation
     */
    @Override
    public String toString() {
        return "Window{" +
                "lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                ", threshold=" + threshold +
                '}';
    }

    /**
     * @return upperBound of the window
     */
    public int getUpperBound() {
        return upperBound;
    }

}
