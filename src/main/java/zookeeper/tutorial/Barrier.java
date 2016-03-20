package zookeeper.tutorial;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by benjamin on 10/24/15.
 */
public class Barrier implements Watcher {
    private final Logger LOG = LoggerFactory.getLogger(Barrier.class);

    private final String hostPort;
    private ZooKeeper zk;
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private volatile boolean barrierSet = false;

    public Barrier(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
        LOG.debug("barrier started");
    }

    void stopZK() throws InterruptedException, IOException {
        zk.close();
        LOG.debug("barrier closed");
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

    public void setBarrier() {
        LOG.info("setting barrier");
        zk.create("/barrier",
                "barrier-id".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                barrierCreateCallback,
                null);
    }

    AsyncCallback.StringCallback barrierCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    LOG.debug("connection was lost while setting the barrier");
                    setBarrier();
                    break;
                case OK:
                case NODEEXISTS:
                    LOG.debug("the barrier is set");
                    barrierSet = true;
                    barrierExists();
                    break;
                default:
                    barrierSet = false;
                    LOG.error("Something went wrong when setting barrier.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("barrier is " + (barrierSet ? "" : "not ") + "set");
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
            if(e.getType() == Event.EventType.NodeDeleted) {
                LOG.info("barrier deleted");

                if ("/barrier".equals( e.getPath() )) {
                    try {
                        Thread.sleep(500);
                        setBarrier();
                    } catch (InterruptedException ie) {
                        LOG.error("failed to sleep", ie);
                    }
                }
            }
        }
    };

    AsyncCallback.StatCallback barrierExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            LOG.debug("Code = {}", KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    barrierExists();
                    break;
                case OK:
                    if (!barrierSet) setBarrier();
                    break;
                case NONODE:
                    barrierSet = false;
                    setBarrier();
                    LOG.info("set the barrier again.");
                    break;
            }
        }
    };

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Barrier barrier = new Barrier(args[0]);

        barrier.startZK();

        // keep checking to see if the we are connected to the ZooKeeper host
        while(!barrier.isConnected()){
            System.out.println("sleeping for connection...");
            Thread.sleep(100);
        }

        barrier.setBarrier(); // call the method to setup the "/barrier" znode

        // keep checking to see if the we are disconnected from the ZooKeeper host
        while(!barrier.isExpired()){
            System.out.println("sleeping for expiration...");
            Thread.sleep(10000);
        }

        barrier.stopZK();
    }

}
