package cs451;

import java.io.*;
import java.util.Arrays;
import java.util.Objects;

/**
 * Class to wrap message (pid, lsn) and the vector clock to be sent (W)
 * Can be extended with any other information
 */
public class MessagePacket implements Serializable {
    private String message; // Message to be sent -> pid, lsn
    private int[] W; // Vector clock

    /**
     * Build a MessagePacket
     * @param message the message to be sent
     * @param w vector clock
     */
    public MessagePacket(String message, int[] w) {
        this.message = message;
        W = w;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return the vector clock
     */
    public int[] getW() {
        return W;
    }

    /**
     * Serialize a MessagePacket object into a byte array to be sent
     * From https://stackoverflow.com/a/3736247
     * @return serialized object as byte array
     * @throws IOException if the serialization can't be done
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(this);
        return out.toByteArray();
    }

    /**
     * Deserialize a byte array into a MessagePacket
     * From https://stackoverflow.com/a/3736247
     * @param data byte array
     * @return the corresponding MessagePacket
     * @throws IOException if the deserialization can't be done
     */
    public static MessagePacket deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (MessagePacket) is.readObject();
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
        MessagePacket that = (MessagePacket) o;
        return message.equals(that.message);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(message);
    }

    /**
     * @return string representation
     */
    @Override
    public String toString() {
        return "MessagePacket{" +
                "message='" + message + '\'' +
                ", W=" + Arrays.toString(W) +
                '}';
    }
}
