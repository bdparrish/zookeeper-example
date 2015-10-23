package zookeeper.barrier;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by bparrish on 10/22/15.
 */
public class Client implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private Random random = new Random(System.currentTimeMillis());
    private static String CLIENT_ZNODE;
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public Client(final String hostPort) {
        this.hostPort = hostPort;
        CLIENT_ZNODE = "/clients/" + serverId;
    }

    public void startZK() throws IOException {
        this.zk = new ZooKeeper(this.hostPort, 15000, this);
    }

    public void stopZK() throws InterruptedException {
        this.zk.close();
    }

    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }

    public void createClientParent() {
        zk.create("/clients",
                serverId.getBytes(),
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
                    barrierWait();
                    break;
                default:
                    LOG.error("there was an issue creating the client parent.");
                    break;
            }
        }
    };

    public void barrierWait() {
        zk.create(CLIENT_ZNODE,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                clientCreateCallback,
                null);
    }

    AsyncCallback.StringCallback clientCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    barrierWait();

                    break;
                case OK:
                case NODEEXISTS:
                    barrierExists();

                    break;
                default:
                    LOG.error("Something went wrong when adding client to barrier.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("Added " + serverId + " to the barrier.");
        }
    };

    public void barrierExists() {
        zk.exists(Barrier.BARRIER_ZNODE,
                barrierExistsWatcher,
                barrierExistsCallback,
                null);
    }

    Watcher barrierExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeDeleted) {
                performAction();
            }
        }
    };

    AsyncCallback.StatCallback barrierExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case NONODE:
                    performAction();
                    break;
            }
        }
    };

    public void performAction() {
        try {
            LOG.info("{}", System.currentTimeMillis());
            Thread.sleep(1000);
            LOG.info("{}", System.currentTimeMillis());
            deleteClient();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void deleteClient() {
        LOG.info("Deleting client " + CLIENT_ZNODE);
        zk.delete(CLIENT_ZNODE,
                -1,
                deleteClientCallback,
                null);
    }

    AsyncCallback.VoidCallback deleteClientCallback = new AsyncCallback.VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path, -1, deleteClientCallback, null);
                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);
                    barrierWait();
                    break;
                default:
                    LOG.error("Something went wrong here, " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * Check if this client is connected.
     *
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * Check if the ZooKeeper session has expired.
     *
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if(zk != null) {
            try{
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn( "Interrupted while closing ZooKeeper session.", e );
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Client client = new Client(args[0]);

        client.startZK();

        while (!client.connected) {
            Thread.sleep(100);
        }

        client.createClientParent();

        while (!client.expired) {
            Thread.sleep(1000);
        }

        client.stopZK();
    }
}
