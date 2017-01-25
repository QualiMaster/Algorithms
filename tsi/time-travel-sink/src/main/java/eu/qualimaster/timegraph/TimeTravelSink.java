package eu.qualimaster.timegraph;

import eu.qualimaster.data.inf.ITimeTravelSink;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by ap0n on 22/7/2016.
 */
public class TimeTravelSink implements ITimeTravelSink {

  private static final String DEFAULT_PROPERTIES_PATH = "/var/nfs/qm/tsi/";
  private static final String DEFAULT_CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final int DEFAULT_CORRELATION_RESULT_SERVER_PORT = 8888;
  private String correlationResultServerIp = "";
  private Integer correlationResultServerPort = -1;
  private static final Logger logger = LoggerFactory.getLogger(TimeTravelSink.class);
  private Socket socket;
  private PrintWriter writer;
  private boolean terminated;

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  public TimeTravelSink() {
    this.terminated = false;
  }

  @Override
  public void postDataSnapshotStream(ITimeTravelSinkSnapshotStreamInput data) {
    if (terminated) return;
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, ITimeTravelSinkSnapshotStreamInput tuple) {
    if (terminated) return;

    logger.info("got snapshot stream: " + tuple.getSnapshot());

    StringBuilder sb = new StringBuilder();
    sb.append("snapshots,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(tuple.getSnapshot());

    try {
      writer.println(sb.toString());
      writer.flush();
      if (writer.checkError()) {
        throw new Exception("Error writing to server.");
      }
    } catch (Exception e) {
      logger.error("Not connected to results server. Reconnecting...");
      try {
        connect();
        writer.println(sb.toString());
        writer.flush();
        if (writer.checkError()) {
          throw new Exception("");
        }
      } catch (Exception ex) {
        logger.error("Can't connect to results server.");
        logger.error(ex.getMessage(), ex);
      }
    }
  }

  @Override
  public void postDataPathStream(ITimeTravelSinkPathStreamInput data) {
    logger.warn("post for pathStreamInput shouldn't have been called");
  }

  @Override
  public void emit(int ticket, ITimeTravelSinkPathStreamInput tuple) {
    logger.warn("emit for pathStreamInput shouldn't have been called");
  }

  @Override
  public void connect() {
    readPropertiesFile();
    try {
      connectToResultsServer();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
//      throw new DefaultModeException(e.getMessage(), e);
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
//      throw new DefaultModeException(e.getMessage(), e);
    }
  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {}

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  private void readPropertiesFile() {
    String externalServicePath = DataManagementConfiguration.getExternalServicePath();
    if (externalServicePath.equals("")) {
      externalServicePath = DEFAULT_PROPERTIES_PATH;
      logger.warn("externalService.path is empty. Using default: " + externalServicePath);
    } else {
      logger.info("Configured externalService.path: " + externalServicePath);
    }
    externalServicePath += "/external-service.properties";
    Properties properties = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(externalServicePath);

      properties.load(inputStream);
      correlationResultServerIp = properties.getProperty("IP");
      if (correlationResultServerIp == null) {
        correlationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
        logger.warn("IP property not found! Using default: " + correlationResultServerIp);
      } else {
        logger.info("Using external-service IP: " + correlationResultServerIp);
      }

      correlationResultServerPort = Integer.parseInt(properties.getProperty("PORT"));
      if (correlationResultServerPort == null) {
        correlationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
        logger.warn("PORT property not found! Using default: " + correlationResultServerPort);
      } else {
        logger.info("Using external-service PORT: " + correlationResultServerPort);
      }
    } catch (IOException ioex) {
      System.err.println(ioex.getMessage());
      ioex.printStackTrace();
    } finally {
      correlationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
      correlationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
      logger.warn("external-service.properties file not found under " + externalServicePath
                  + ". Using default IP: " + correlationResultServerIp
                  + " and PORT: " + correlationResultServerPort);

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

  private void connectToResultsServer() throws IOException {
    socket = new Socket(correlationResultServerIp, correlationResultServerPort);
    writer = new PrintWriter(socket.getOutputStream());
  }
}
