package cs451;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class Window {
    private int lowerBound;
    private int upperBound;
    private ArrayList<Boolean> ackPack;
    private int threshold = Constants.INIT_THRESH;

    public Window(int windowSize) {
        this.lowerBound = 1;
        this.upperBound = windowSize;
        this.ackPack = new ArrayList<>(windowSize);
        for (int i = 0; i < upperBound; i++)
            ackPack.add(false);
    }

    public boolean canSend(int lsn) {
        return lsn >= lowerBound && lsn <= upperBound && !ackPack.get(lsn - lowerBound);
    }


    public boolean alreadyAck(int lsn) {
        if (lsn > upperBound)
            return false;
        return lsn < lowerBound || !ackPack.get(lsn-lowerBound);
    }

    public void markPacket(int lsn) {
        assert (lsn - lowerBound >=0);
        ackPack.set(lsn - lowerBound, true);
        moveWindow();
    }

    private void moveWindow() {
        int numAck = 0;
        for (boolean mark: ackPack) {
            if (mark)
                numAck+=1;
            else
                break;
        }
        if (numAck!=0) {
            lowerBound += numAck;
            upperBound += numAck;
            if (numAck >= 0) {
                ackPack.subList(0, numAck).clear();
                for (int i = 0; i < numAck; i++)
                    ackPack.add(false);
            }
        }
    }

    public void increaseSize() {
        if ((upperBound - lowerBound + 1)>=threshold) {
            if (upperBound - lowerBound + 1 == ackPack.size())
                ackPack.add(false);
            upperBound++;
        }
        else {
            int toAdd = Math.max(2 * (upperBound - lowerBound + 1) - ackPack.size(), 0);
            upperBound += upperBound - lowerBound + 1;
            for (int i = 0; i < toAdd; i++)
                ackPack.add(false);
        }
    }

    public void timeoutStart() {
        threshold = Math.max((upperBound - lowerBound + 1)/2, Constants.WINDOW_SIZE);
        upperBound = lowerBound + Constants.WINDOW_SIZE - 1;
        //upperBound = lowerBound + threshold - 1;
    }

    public void dupAck() {
        threshold = Math.max((upperBound - lowerBound + 1)/2, Constants.WINDOW_SIZE);
        upperBound = lowerBound + threshold - 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return lowerBound == window.lowerBound &&
                upperBound == window.upperBound &&
                ackPack.equals(window.ackPack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound, ackPack, threshold);
    }

    @Override
    public String toString() {
        return "Window{" +
                "lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                //", needAck = " + ackPack.subList(0, upperBound - lowerBound + 1).stream().filter(a -> !a).count() +
//                ", ackPack=" + ackPack +
                ", threshold=" + threshold +
                '}';
    }

    public int getUpperBound() {
        return upperBound;
    }

    public int getWindowSize() {
        return upperBound - lowerBound + 1;
    }
}
