package eu.qualimaster.algorithms.imp;

import eu.qualimaster.data.inf.ITransferSink;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.sinks.IDataSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.Properties;

/**
 * Created by justme on 8/11/2016.
 */
public class TransferSink implements ITransferSink, IDataSink {

  private static final String DEFAULT_PROPERTIES_PATH = "/var/nfs/qm/tsi/";
  private static final String DEFAULT_CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final int DEFAULT_CORRELATION_RESULT_SERVER_PORT = 8888;

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  private String correlationResultServerIp = "";
  private Integer correlationResultServerPort = -1;
  private String replayCorrelationResultServerIp = "";
  private Integer replayCorrelationResultServerPort = -1;
  private Logger logger = LoggerFactory.getLogger(TransferSink.class);
  private Socket socket, replaySocket;
  private PrintWriter writer, replayWriter;
  private boolean terminating;

  public TransferSink() {
    readPropertiesFile();
    terminating = false;
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

      replayCorrelationResultServerIp = properties.getProperty("REPLAY_IP");
      if (replayCorrelationResultServerIp == null) {
        replayCorrelationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
        logger.warn("IP property not found! Using default: " + replayCorrelationResultServerIp);
      } else {
        logger.info("Using replay external-service IP: " + replayCorrelationResultServerIp);
      }

      replayCorrelationResultServerPort = Integer.parseInt(properties.getProperty("REPLAY_PORT"));
      if (replayCorrelationResultServerPort == null) {
        replayCorrelationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
        logger.warn("PORT property not found for replay! Using default: "
                    + replayCorrelationResultServerPort);
      } else {
        logger.info("Using replay external-service PORT: " + replayCorrelationResultServerPort);
      }

    } catch (IOException ioex) {
      ioex.printStackTrace();

      correlationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
      correlationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
      replayCorrelationResultServerIp = DEFAULT_CORRELATION_RESULT_SERVER_IP;
      replayCorrelationResultServerPort = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
      logger.warn("external-service.properties file not found under " + externalServicePath
                  + ". Using default IP: " + correlationResultServerIp
                  + ", PORT: " + correlationResultServerPort
                  + ", replay IP: " + replayCorrelationResultServerIp
                  + ", replay PORT: " + replayCorrelationResultServerPort);
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
  public void postDataPairwiseFinancial(ITransferSinkPairwiseFinancialInput data) {
    sendToServer(-1, data, false);
  }

  @Override
  public void emit(int ticket, ITransferSinkPairwiseFinancialInput data) {
    sendToServer(ticket, data, true);
  }

  private void sendToServer(int ticket, ITransferSinkPairwiseFinancialInput data,
                            boolean isReplay) {
    if (terminating) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("te,");
    if (ticket != -1) {
      sb.append("te_replay_response,").append(ticket).append(",");
    } else {
      sb.append("te_response,");
    }
    sb.append(data.getId0()).append(",").append(data.getId1()).append(",").append(data.getDate())
        .append(",")
        .append(new DecimalFormat("0.######").format(data.getValue()));
    sendStr(sb.toString(), isReplay);
  }

  private void sendStr(String str, boolean isReplay) {
    PrintWriter w = isReplay ? replayWriter : writer;
    try {
      w.println(str);
      if (w.checkError()) {
        throw new Exception("Error");
      }
    } catch (Exception e) {
      String server = isReplay ? "replay" : "result";
      logger.error("Error. Disconnected from " + server + " server. Reconnecting...");
      try {
        if (isReplay) {
          connectToReplayServer();
          w = replayWriter;
        } else {
          connectToNormalServer();
          w = writer;
        }
        w.println(str);
        if (w.checkError()) {
          throw new IOException("Error writing to socket.");
        }
      } catch (IOException e1) {
        logger.error(e1.getMessage(), e1);
      }
    }
  }

  private void connectToResultsServer() throws IOException {
    connectToNormalServer();
    connectToReplayServer();
  }

  private void connectToReplayServer() throws IOException {
    replaySocket = new Socket(replayCorrelationResultServerIp, replayCorrelationResultServerPort);
    replayWriter = new PrintWriter(replaySocket.getOutputStream(), true);
  }

  private void connectToNormalServer() throws IOException {
    socket = new Socket(correlationResultServerIp, correlationResultServerPort);
    writer = new PrintWriter(socket.getOutputStream(), true);
  }

  @Override
  public void connect() {
    try {
      terminating = false;
      connectToResultsServer();
    } catch (IOException e) {
      // Ignore exception, means could not connect to external service. Continue
      e.printStackTrace();
      //      throw new DefaultModeException(e.getMessage());
    }
  }

  @Override
  public void disconnect() {
    terminating = true;
    closeQuietly(writer);
    closeQuietly(socket);
    closeQuietly(replayWriter);
    closeQuietly(replaySocket);
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

  private void closeQuietly(java.io.Closeable closable) {
    if (closable != null) {
      try {
        closable.close();
      } catch (IOException e) {
        // Ignore the exception
      }
    }
  }
}
