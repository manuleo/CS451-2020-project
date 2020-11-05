package cs451;
import java.util.Arrays;
import java.util.Objects;

public class Window {
    private int lowerBound;
    private int upperBound;
    private boolean[] ackPack;

    public Window(int windowSize) {
        this.lowerBound = 1;
        this.upperBound = windowSize;
        this.ackPack = new boolean[windowSize];
        Arrays.fill(this.ackPack, false);
    }

    public boolean canSend(Packet p) {
        int lsn = Integer.parseInt(p.getMessage().split(" ")[1]);
        return lsn >= lowerBound && lsn <= upperBound && !ackPack[lsn - lowerBound];
    }

    public void markPacket(Packet p) {
        int lsn = Integer.parseInt(p.getMessage().split(" ")[1]);
        assert (lsn - lowerBound >=0);
        ackPack[lsn - lowerBound] = true;
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
                System.arraycopy(ackPack, numAck, ackPack, 0, ackPack.length - numAck);
                for (int i = ackPack.length - numAck; i < ackPack.length; i++)
                    ackPack[i] = false;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return lowerBound == window.lowerBound &&
                upperBound == window.upperBound &&
                Arrays.equals(ackPack, window.ackPack);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(lowerBound, upperBound);
        result = 31 * result + Arrays.hashCode(ackPack);
        return result;
    }

    @Override
    public String toString() {
        return "Window{" +
                "lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                //", ackPack=" + Arrays.toString(ackPack) +
                '}';
    }

    public int getUpperBound() {
        return upperBound;
    }
}
