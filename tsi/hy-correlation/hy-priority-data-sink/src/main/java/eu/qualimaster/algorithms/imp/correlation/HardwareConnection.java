package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons.Receiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

import backtype.storm.utils.Utils;

/**
 * Created by Apostolos Nydriotis on 2015/09/15.
 */
public class HardwareConnection implements Runnable {

  private static final String OLYNTHOS_IP = "147.27.39.12";
//  private static final String VERGINA_IP = "147.27.39.13";
  private static final int FINANCIAL_RECEIVER_PORT = 2401;
//  private static final int TWITTER_RECEIVER_PORT = 2403;
//  private static final String TWITTER_PREFIX = "t,";
  private static final String FINANCIAL_PREFIX = "f,";
  private Logger logger = LoggerFactory.getLogger(HardwareConnection.class);
  private PrintWriter writer;

  private Receiver financialConnection;
//  private Receiver twitterConnection;

  private long financialMonitoringTimestamp;
  private long financialThroughput;
  private int measurementDuration;  // seconds
  private Object writerLock;

  volatile private boolean running;

  public HardwareConnection(PrintWriter writer, Object writerLock) throws IOException {
    running = true;

    financialMonitoringTimestamp = 0L;
    financialThroughput = 0L;
    measurementDuration = 1 * 60;

    this.writer = writer;
    this.writerLock = writerLock;

    try {
      Utils.sleep(5000);
      financialConnection = new Receiver(OLYNTHOS_IP, FINANCIAL_RECEIVER_PORT);
    } catch (Exception e) {
      // TODO(ap0n): Handle me
      logger.error("Couldn't connect to " + OLYNTHOS_IP, e);
    }
//    try {
//      twitterConnection = new Receiver(OLYNTHOS_IP, TWITTER_RECEIVER_PORT);
//    } catch (Exception e) {
//      // TODO(ap0n): Handle me
//      logger.error("Couldn't connect to " + OLYNTHOS_IP, e);
//      e.printStackTrace();
//    }
  }

  @Override
  public void run() {
    logger.info("thread started");
    String financialResult = null;
    String twitterResult = null;

    while (running) {
      try {
        financialResult = financialConnection.receiveData();
      } catch (Exception e) {
        logger.error("(Financial) Communication error", e);
        if (financialConnection == null) {
          logger.error("(Financial) Reconnecting to " + OLYNTHOS_IP);
          try {
            Utils.sleep(1000);
            financialConnection = new Receiver(OLYNTHOS_IP, FINANCIAL_RECEIVER_PORT);
          } catch (Exception e1) {
            logger.error("(Financial) Could not reconnect to " + OLYNTHOS_IP, e1);
          }
        }
      }
      if (financialResult != null) {
//          monitorMe();
        synchronized (writerLock) {
          writer.println(FINANCIAL_PREFIX + financialResult);
        }
      }
//      try {
//        twitterResult = twitterConnection.receiveData();
//      } catch (Exception e) {
//        // TODO(ap0n): Handle me
//        logger.error("(Twitter) Communication error", e);
//        if (twitterConnection == null) {
//          logger.error("(Twitter) Reconnecting to " + OLYNTHOS_IP);
//          try {
//            Utils.sleep(1000);
//            financialConnection = new Receiver(OLYNTHOS_IP, TWITTER_RECEIVER_PORT);
//          } catch (Exception e1) {
//            logger.error("(Twitter) Could not reconnect to " + OLYNTHOS_IP, e1);
//          }
//        }
//      }
//      if (twitterResult != null) {
//        synchronized (writerLock) {
//          writer.println(TWITTER_PREFIX + twitterResult);
//        }
//      }

      if (financialResult == null && twitterResult == null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          logger.error("Interrupted Error", e);
        }
      }
    }
  }

  public void terminate() {
    running = false;
  }

//  private void monitorMe() {
//    if (financialMonitoringTimestamp == 0) {
//      financialMonitoringTimestamp = new Date().getTime();
//      ++financialThroughput;
//    } else {
//      long now = new Date().getTime();
//      if (now - financialMonitoringTimestamp < measurementDuration * 1000) {
//        ++financialThroughput;
//      } else {
//        logger.info("Pipeline financial output throughput: "
//                    + ((double) financialThroughput / (double) measurementDuration)
//                    + " tuples/sec");
//        financialMonitoringTimestamp = now;
//        financialThroughput = 1;
//      }
//    }
//  }
}
