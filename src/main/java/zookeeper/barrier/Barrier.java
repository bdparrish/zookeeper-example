package zookeeper.barrier;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

/**
 * Created by bparrish on 10/22/15.
 */
public class Barrier implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Barrier.class);

    enum BarrierStates { BARRIER_SET, BARRIER_NOT_SET };

    private static volatile BarrierStates state = BarrierStates.BARRIER_NOT_SET;

    public static BarrierStates getState() {
        return state;
    }

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */

    public static String BARRIER_ZNODE = "/barrier";

    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private final String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    /**
     * Creates a new master instance.
     *
     * @param hostPort
     */
    Barrier(String hostPort) {
        this.hostPort = hostPort;
    }

    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
        LOG.debug("barrier started");
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
        LOG.debug("barrier closed");
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

    public void setBarrier() {
        LOG.info("Setting barrier");
        zk.create(BARRIER_ZNODE,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                barrierCreateCallback,
                null);
    }

    AsyncCallback.StringCallback barrierCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    setBarrier();
                    break;
                case OK:
                case NODEEXISTS:
                    state = BarrierStates.BARRIER_SET;
                    barrierExists();
                    break;
                default:
                    state = BarrierStates.BARRIER_NOT_SET;
                    LOG.error("Something went wrong when setting barrier.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("Barrier is " + (state == BarrierStates.BARRIER_SET ? "" : "not ") + "set");
        }
    };

    public void barrierExists() {
        zk.exists(BARRIER_ZNODE,
                barrierExistsWatcher,
                barrierExistsCallback,
                null);
    }

    AsyncCallback.StatCallback barrierExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            LOG.debug("Code = {}", KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    barrierExists();

                    break;
                case OK:
                    if (state == BarrierStates.BARRIER_NOT_SET) setBarrier();
                    break;
                case NONODE:
                    state = BarrierStates.BARRIER_NOT_SET;
                    setBarrier();
                    LOG.info("Set the barrier again.");

                    break;
                default:
                    checkBarrier();
                    break;
            }
        }
    };

    Watcher barrierExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeDeleted) {
                LOG.info("Barrier deleted");
                assert BARRIER_ZNODE.equals( e.getPath() );

                try {
                    Thread.sleep(100);
                    setBarrier();
                } catch (InterruptedException ie) {
                    LOG.error("failed to sleep", ie);
                }
            }
        }
    };

    public void checkBarrier() {
        zk.getData(BARRIER_ZNODE, false, barrierCheckCallback, null);
    }

    AsyncCallback.DataCallback barrierCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkBarrier();

                    break;
                case NONODE:
                    setBarrier();

                    break;
                case OK:
                    state = BarrierStates.BARRIER_NOT_SET;
                    if( serverId.equals( new String(data) ) ) {
                        setBarrier();
                    } else {
                        barrierExists();
                    }

                    break;
                default:
                    LOG.error("Error when reading data.",
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
        Barrier barrier = new Barrier(args[0]);

        barrier.startZK();

        while(!barrier.isConnected()){
            Thread.sleep(100);
        }

        barrier.setBarrier();

        while(!barrier.isExpired()){
            Thread.sleep(1000);
        }

        barrier.stopZK();
    }

}
