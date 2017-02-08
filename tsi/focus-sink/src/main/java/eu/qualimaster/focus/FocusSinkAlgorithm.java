package eu.qualimaster.focus;

import eu.qualimaster.data.inf.IFocusSink;
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

//import org.apache.log4j.LogManager;

/**
 * Created by ap0n on 10/2/2016.
 */
public class FocusSinkAlgorithm implements IFocusSink {

  private static final String DEFAULT_PROPERTIES_PATH = "/var/nfs/qm/tsi/";
  private static final String DEFAULT_CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final int DEFAULT_CORRELATION_RESULT_SERVER_PORT = 8888;

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  private String correlationResultServerIp = "";
  private Integer correlationResultServerPort = -1;
  private Logger logger = LoggerFactory.getLogger(FocusSinkAlgorithm.class);
  private Socket socket;
  private boolean terminated;
  private PrintWriter writer;

  public FocusSinkAlgorithm() {
    this.terminated = false;
	System.out.println("------sys------FocusSinkAlgorithm is initiated");
	logger.info("------------FocusSinkAlgorithm is initiated");
  }

  @Override
  public void postDataRecommendationStream(IFocusSinkRecommendationStreamInput data) {
	System.out.println("------post_sys------Focus Sink receives String: "+ data.getRecommendations());
	logger.info("------post------Focus Sink receives String: "+ data.getRecommendations());
	
    emit(-1, data);
  }

  @Override
  public void emit(int ticket,
                   IFocusSinkRecommendationStreamInput data) {
    if (terminated || data == null || data.getRecommendations() == null || data.getRecommendations()
        .equals("")) {
      return;
    }
	
	System.out.println("------++++_sys------Focus Sink receives String: "+ data.getRecommendations());
	logger.info("------++++------Focus Sink receives String: "+ data.getRecommendations());
	
    StringBuilder sb = new StringBuilder();
    sb.append("focusPip,suggestion_response,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getRecommendations());
	
	logger.info("------++++------Focus Sink sends String: "+ sb.toString());

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
	
	System.out.println("------financial------Focus Sink receives data");
	logger.info("------financial------Focus Sink receives data");

    StringBuilder sb = new StringBuilder();
    sb.append("focusPip,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append("resultsNodes_response,").append(data.getEdge());

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
      logger.error(ioex.getMessage(), ioex);

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

  private void connectToResultsServer() throws IOException {
    logger.info("Connecting to results server");
    socket = new Socket(correlationResultServerIp, correlationResultServerPort);
    logger.info("Connected. IP: " + socket.getInetAddress().getHostAddress()
                + " PORT:" + socket.getPort());
    writer = new PrintWriter(socket.getOutputStream(), true);
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
