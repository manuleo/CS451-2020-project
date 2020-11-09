package cs451;

import java.sql.Connection;
import java.util.List;

public class Parser {

    private String[] args;
    private long pid;
    private IdParser idParser;
    private HostsParser hostsParser;
    private BarrierParser barrierParser;
    private SignalParser signalParser;
    private OutputParser outputParser;
    private ConfigParser configParser;

    public Parser(String[] args) {
        this.args = args;
    }

    public void parse() {
        pid = ProcessHandle.current().pid();

        idParser = new IdParser();
        hostsParser = new HostsParser();
        barrierParser = new BarrierParser();
        signalParser = new SignalParser();
        outputParser = new OutputParser();
        configParser = null;


        if (!idParser.populate(args[Constants.ID_KEY], args[Constants.ID_VALUE])) {
            help();
        }

        if (!hostsParser.populate(args[Constants.HOSTS_KEY], args[Constants.HOSTS_VALUE])) {
            help();
        }

        if (!hostsParser.inRange(idParser.getId())) {
            help();
        }

        if (!barrierParser.populate(args[Constants.BARRIER_KEY], args[Constants.BARRIER_VALUE])) {
            help();
        }

        if (!signalParser.populate(args[Constants.SIGNAL_KEY], args[Constants.SIGNAL_VALUE])) {
            help();
        }

        if (!outputParser.populate(args[Constants.OUTPUT_KEY], args[Constants.OUTPUT_VALUE])) {
            help();
        }

        configParser = new ConfigParser();
        if (!configParser.populate(args[Constants.CONFIG_VALUE])) {
        }

        Constants.WINDOW_SIZE = Integer.parseInt(args[11]);
        Constants.INIT_THRESH = Integer.parseInt(args[12]);
    }

    private void help() {
        System.err.println("Usage: ./run.sh --id ID --hosts HOSTS --barrier NAME:PORT --signal NAME:PORT --output OUTPUT [config]");
        System.exit(1);
    }

    public int myId() {
        return idParser.getId();
    }

    public List<Host> hosts() {
        return hostsParser.getHosts();
    }

    public String barrierIp() {
        return barrierParser.getIp();
    }

    public int barrierPort() {
        return barrierParser.getPort();
    }

    public String signalIp() {
        return signalParser.getIp();
    }

    public int signalPort() {
        return signalParser.getPort();
    }

    public String output() {
        return outputParser.getPath();
    }

    public boolean hasConfig() {
        return configParser != null;
    }

    public String config() {
        return configParser.getPath();
    }

}
