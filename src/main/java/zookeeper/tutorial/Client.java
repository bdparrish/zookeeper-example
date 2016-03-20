package zookeeper.tutorial;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by benjamin on 10/24/15.
 */
public class Client implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final String hostPort;
    private ZooKeeper zk;
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public Client(final String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
        LOG.debug("client started");
    }

    void stopZK() throws InterruptedException, IOException {
        zk.close();
        LOG.debug("client closed");
    }

    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                    LOG.debug("connected to zookeeper");
                    connected = true;
                    break;
                case Disconnected:
                    LOG.debug("disconnected from zookeeper");
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("session expiration");
                default:
                    break;
            }
        }
    }

    public void createClientParent() {
        zk.create("/clients",
                "clients-id".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createClientParentCallback,
                null);
    }

    AsyncCallback.StringCallback createClientParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createClientParent();
                    break;
                case OK:
                case NODEEXISTS:
                    createClient();
                    break;
                default:
                    LOG.error("there was an issue creating the client parent.");
                    break;
            }
        }
    };

    public void createClient() {
        int randomInt = new Random(System.currentTimeMillis()).nextInt();
        String hex = Integer.toHexString(randomInt);
        zk.create("/clients/" + hex,
                hex.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                clientCreateCallback,
                null);
    }

    AsyncCallback.StringCallback clientCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createClient();
                    break;
                case OK:
                case NODEEXISTS:
                    barrierExists();
                    break;
                default:
                    LOG.error("something went wrong when adding client to barrier.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("added client to the barrier.");
        }
    };

    public void barrierExists() {
        zk.exists("/barrier",
                barrierExistsWatcher,
                barrierExistsCallback,
                null);
    }

    Watcher barrierExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeDeleted) {
//                performAction();
                LOG.info("barrier removed - client taking action");
            }
        }
    };

    AsyncCallback.StatCallback barrierExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    barrierExists();
                    break;
            }
        }
    };

    public void performAction() {
        try {
            LOG.info("{}", System.currentTimeMillis());
            Thread.sleep(1000);
            LOG.info("{}", System.currentTimeMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Client client = new Client(args[0]);

        client.startZK();

        while (!client.isConnected()) {
            System.out.println("sleeping for connection...");
            Thread.sleep(100);
        }

        client.createClientParent();

        while (!client.isExpired()) {
            System.out.println("sleeping for expiration...");
            Thread.sleep(10000);
        }

        client.stopZK();
    }

}
