package zookeeper.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Producer implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    public static String QUEUE_ZNODE_ITEM = "/queue-";

    private ZooKeeper zk;
    private String hostPort;
    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public Producer(final String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        this.zk = new ZooKeeper(this.hostPort, 15000, this);
    }

    public void stopZK() throws InterruptedException, IOException {
        close();
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

    public void queueExists() {
        zk.exists(Queue.QUEUE_ZNODE,
                queueExistsWatcher,
                queueExistsCallback,
                null);
    }

    Watcher queueExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            LOG.info("Processing event: " + e.toString());
            if (e.getType() == Event.EventType.NodeCreated) {
                createQueueItem();
            }
        }
    };

    AsyncCallback.StatCallback queueExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            LOG.info("Processing result: " + KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    createQueueItem();
                    break;
            }
        }
    };

    public void createQueueItem() {
        try {
            for (int i = 0; i < 10; i++) {
                LOG.info("Creating queue item: " + i);
                zk.create(Queue.QUEUE_ZNODE + QUEUE_ZNODE_ITEM,
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL
                );
                serverId = Integer.toHexString( random.nextInt() );
            }
        } catch (KeeperException | InterruptedException ex) {
            LOG.error(ex.getLocalizedMessage(), ex);
        }
    }

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
        Producer producer = new Producer(args[0]);

        producer.startZK();

        while (!producer.isConnected()) {
            Thread.sleep(100);
        }

        producer.queueExists();

        while (!producer.isExpired()) {
            Thread.sleep(1000);
        }

        producer.stopZK();
    }
}
