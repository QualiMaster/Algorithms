package eu.qualimaster.algorithms.imp.correlation;

import org.apache.log4j.Logger;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.curator.retry.RetryOneTime;
import org.apache.storm.zookeeper.data.Stat;

/**
 * Created by Apostolos Nydriotis on 2/2/15.
 */
public class SignalClient {

  final static Logger logger = Logger.getLogger(SignalClient.class);

  private CuratorFramework client = null;
  private String name;

  public SignalClient(String zkConnectString, String name, String namespace) {
    this.name = name;
    this.client =
        CuratorFrameworkFactory.builder().namespace(namespace).connectString(zkConnectString)
            .retryPolicy(new RetryOneTime(500)).build();

    logger.debug("created Curator client");
  }

  public void start() {
    this.client.start();
  }

  public void close() {
    this.client.close();
  }

  public void send(byte[] signal) throws Exception {
    Stat stat = this.client.checkExists().forPath(this.name);
    if (stat == null) {
      String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
      logger.info("Created: " + path);
    }
    this.client.setData().forPath(this.name, signal);
  }
}
