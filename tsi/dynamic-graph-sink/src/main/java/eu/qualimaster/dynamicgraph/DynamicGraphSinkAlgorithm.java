package eu.qualimaster.dynamicgraph;

import eu.qualimaster.data.inf.IDynamicGraphSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by ap0n on 8/2/2016.
 */
public class DynamicGraphSinkAlgorithm implements IDynamicGraphSink {

//  private static final String CORRELATION_RESULT_SERVER_IP = "snf-618466.vm.okeanos.grnet.gr";
  //  private static final String CORRELATION_RESULT_SERVER_IP = "147.27.14.117";
private static String CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final String PROPERTIES_PATH = "/var/nfs/qm/tsi/external-service.properties";
  private static int CORRELATION_RESULT_SERVER_PORT = 8888;
  private Logger logger = LoggerFactory.getLogger(DynamicGraphSinkAlgorithm.class);
  private Socket socket;
  private PrintWriter writer;
  private boolean terminated;

  public DynamicGraphSinkAlgorithm() {
    this.terminated = false;
  }

  @Override
  public void postDataHubStream(IDynamicGraphSinkHubStreamInput data) {
    if (terminated) return;
    emit(-1, data);
  }

  @Override
  public void emit(int ticket,
                   IDynamicGraphSinkHubStreamInput data) {
    if (terminated) return;
    StringBuilder sb = new StringBuilder();

    sb.append("hubList,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getHubList());

    try {
      writer.println(sb.toString());
      if (writer.checkError()) {
        throw new Exception("Error writing...");
      }
    } catch (Exception e) {
      logger.error("Not connected to results server. Reconnecting...");
      try {
        connect();
        writer.println(sb.toString());
        if (writer.checkError()) {
          throw new Exception("Error writing...");
        }
      } catch (Exception ex) {
        logger.error("Can't connect to results server.");
        logger.error(ex.getMessage(), ex);
      }
    }
  }

  @Override
  public void connect() {
    readPropertiesFile();
    try {
      connectToResultsServer();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
//      throw new DefaultModeException(e.getMessage());
    }
  }

  private void readPropertiesFile() {
    Properties properties = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(PROPERTIES_PATH);
      properties.load(inputStream);
      CORRELATION_RESULT_SERVER_IP = properties.getProperty("IP");
      CORRELATION_RESULT_SERVER_PORT = Integer.parseInt(properties.getProperty("PORT"));
    } catch (IOException ioex) {
      System.err.println(ioex.getMessage());
      ioex.printStackTrace();
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException ex) {
          // Ignore exception, means file not found or something similar. Fall back to defaults.
          //          ex.printStackTrace();
        }
      }
    }
  }

  @Override
  public void disconnect() {
    terminated = true;
    writer.close();
    try {
      socket.close();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
//      throw new DefaultModeException(e.getMessage());
    }
  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  private void connectToResultsServer() throws IOException {
    socket = new Socket(CORRELATION_RESULT_SERVER_IP, CORRELATION_RESULT_SERVER_PORT);
    writer = new PrintWriter(socket.getOutputStream(), true);
  }
}
