package zookeeper.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class Queue implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */

    public static String QUEUE_ZNODE = "/_QUEUE_";

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
    Queue(String hostPort) {
        this.hostPort = hostPort;
    }

    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
        LOG.debug("queue started");
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
        LOG.debug("queue closed");
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

    public void createQueue() {
        LOG.info("Creating queue");
        zk.create(QUEUE_ZNODE,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                queueCreateCallback,
                null);
    }

    AsyncCallback.StringCallback queueCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createQueue();
                    break;
                case OK:
                case NODEEXISTS:
                    LOG.info("Queue exists.");
                    break;
                default:
                    LOG.error("Something went wrong when setting barrier.",
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
        Queue queue = new Queue(args[0]);

        queue.startZK();

        while(!queue.isConnected()){
            Thread.sleep(100);
        }

        queue.createQueue();

        while(!queue.isExpired()){
            Thread.sleep(1000);
        }

        queue.stopZK();
    }

}
