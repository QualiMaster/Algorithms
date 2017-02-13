package eu.qualimaster.timegraph;

import eu.qualimaster.data.inf.ITimeTravelSink;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
  private static final Logger logger = LoggerFactory.getLogger(TimeTravelSink.class);

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  private String correlationResultServerIp = "";
  private Integer correlationResultServerPort = -1;
  private Socket socket;
  private PrintWriter writer;
  private boolean terminated;

  public TimeTravelSink() {
    this.terminated = false;
  }

  @Override
  public void postDataSnapshotStream(ITimeTravelSinkSnapshotStreamInput data) {
    if (terminated) {
      return;
    }
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, ITimeTravelSinkSnapshotStreamInput tuple) {
    if (terminated) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("snapshots,snapshot_response,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(tuple.getSnapshot());
    sendToServer(sb.toString());
  }

  @Override
  public void postDataPathStream(ITimeTravelSinkPathStreamInput data) {
    if (terminated) {
      return;
    }
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, ITimeTravelSinkPathStreamInput tuple) {
    if (terminated) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("snapshots,path_response,");  // Server doesn't separate snapshots from paths (actually doesn't know
                              // about paths).
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(tuple.getPath());
    sendToServer(sb.toString());
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
    closeQuietly(writer);
    closeQuietly(socket);
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

  private void sendToServer(String result) {
    try {
      writer.println(result);
      writer.flush();
      if (writer.checkError()) {
        throw new Exception("Error writing to server.");
      }
    } catch (Exception e) {
      logger.error("Not connected to results server. Reconnecting...");
      try {
        connect();
        writer.println(result);
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
      correlationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
      correlationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
      logger.warn("external-service.properties file not found under " + externalServicePath
                  + ". Using default IP: " + correlationResultServerIp
                  + " and PORT: " + correlationResultServerPort);
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

  private void connectToResultsServer() throws IOException {
    socket = new Socket(correlationResultServerIp, correlationResultServerPort);
    writer = new PrintWriter(socket.getOutputStream());
  }

  private void closeQuietly(Closeable closable) {
    if (closable != null) {
      try {
        closable.close();
      } catch (IOException e) {
        // Ignore the exception
      }
    }
  }
}
