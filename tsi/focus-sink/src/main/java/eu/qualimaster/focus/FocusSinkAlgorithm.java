package eu.qualimaster.focus;

import eu.qualimaster.data.inf.IFocusSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

//import org.apache.log4j.LogManager;

/**
 * Created by ap0n on 10/2/2016.
 */
public class FocusSinkAlgorithm implements IFocusSink {

  private static final String PROPERTIES_PATH = "/var/nfs/qm/tsi/external-service.properties";
  private static String CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static int CORRELATION_RESULT_SERVER_PORT = 8888;
  private Logger logger = LoggerFactory.getLogger(FocusSinkAlgorithm.class);
  private Socket socket;
  private boolean terminated;
  private PrintWriter writer;

  public FocusSinkAlgorithm() {
    this.terminated = false;
  }

  @Override
  public void postDataRecommendationStream(IFocusSinkRecommendationStreamInput data) {
    emit(-1, data);
  }

  @Override
  public void emit(int ticket,
                   IFocusSinkRecommendationStreamInput data) {
    if (terminated || data == null || data.getRecommendations() == null || data.getRecommendations()
        .equals("")) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("focusPip,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getRecommendations());

    sendStr(sb.toString());
  }

  @Override
  public void postDataEdgeStream(IFocusSinkEdgeStreamInput data) {
    if (terminated) {
      return;
    }
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, IFocusSinkEdgeStreamInput data) {
    if (terminated || data == null || data.getEdge() == null || data.getEdge().equals("")) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("focusPip,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append("resultsNodes_response,f,").append(data.getEdge());

    sendStr(sb.toString());
  }

  @Override
  public void connect() {
    readPropertiesFile();
    try {
      connectToResultsServer();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }

  private void sendStr(String str) {
    try {
      writer.println(str);
      if (writer.checkError()) {
        throw new Exception("Error writing...");
      }
    } catch (Exception e) {
      logger.error("Not connected to results server. Reconnecting...");
      try {
        connect();
        writer.println(str);
        if (writer.checkError()) {
          throw new Exception("Error writing...");
        }
      } catch (Exception ex) {
        logger.error("Can't connect to results server.");
        logger.error(ex.getMessage(), ex);
      }
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
      logger.error(ioex.getMessage(), ioex);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException ex) {
          logger.warn(
              PROPERTIES_PATH + " not found! Using default IP: " + CORRELATION_RESULT_SERVER_IP
              + " PORT: " + CORRELATION_RESULT_SERVER_PORT);
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
    logger.info("Connecting to results server");
    socket = new Socket(CORRELATION_RESULT_SERVER_IP, CORRELATION_RESULT_SERVER_PORT);
    logger.info("Connected. IP: " + socket.getInetAddress().getHostAddress()
                + " PORT:" + socket.getPort());
    writer = new PrintWriter(socket.getOutputStream(), true);
  }
}
