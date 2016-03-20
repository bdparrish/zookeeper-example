package zookeeper.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class Consumer implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private ZooKeeper zk;
    private String hostPort;
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public Consumer(final String hostPort) {
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

    public void processQueue() {
        try {
            List<String> children = zk.getChildren(Queue.QUEUE_ZNODE, true);

            Collections.sort(children);

            for (String item : children) {
                String path = Queue.QUEUE_ZNODE + "/" + item;
                zk.getData(path,
                        false,
                        dataCallback,
                        null);
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Error getting children for znode: " + Queue.QUEUE_ZNODE, e);
        }
    }

    AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            LOG.info("Processing data: " + KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.info(new String(data));
                    deleteQueueItem(path);
                    break;
            }
        }
    };

    public void deleteQueueItem(String path) {
        LOG.info("Deleting queue item: " + path);
        zk.delete(path, -1, deleteQueueItemCallback, null);
    }

    AsyncCallback.VoidCallback deleteQueueItemCallback = new AsyncCallback.VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path, -1, deleteQueueItemCallback, null);
                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);
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
        Consumer consumer = new Consumer(args[0]);

        consumer.startZK();

        while (!consumer.isConnected()) {
            Thread.sleep(100);
        }

        consumer.processQueue();

        while (!consumer.isExpired()) {
            Thread.sleep(1000);
        }

        consumer.stopZK();
    }
}
