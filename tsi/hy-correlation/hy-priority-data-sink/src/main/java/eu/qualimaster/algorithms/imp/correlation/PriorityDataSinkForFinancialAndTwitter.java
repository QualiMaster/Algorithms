package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.data.inf.IPriorityDataSink;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.sinks.IDataSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;


/**
 * Created by Nikolaos Pavlakis on 1/16/15.
 */
public class PriorityDataSinkForFinancialAndTwitter implements IPriorityDataSink, IDataSink {

  private static final String DEFAULT_PROPERTIES_PATH = "/var/nfs/qm/tsi/";
  private static final String DEFAULT_CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final int DEFAULT_CORRELATION_RESULT_SERVER_PORT = 8888;
  private String correlation_result_server_ip = "";
  private Integer correlation_result_server_port = -1;
  private Logger logger = LoggerFactory.getLogger(PriorityDataSinkForFinancialAndTwitter.class);
  private Socket socket;
  private PrintWriter writer;
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");

  private long financialMonitoringTimestamp;
  private long financialThroughput;
  private int measurementDuration;  // seconds
  private boolean terminating;

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  public PriorityDataSinkForFinancialAndTwitter() {
    financialMonitoringTimestamp = 0L;
    financialThroughput = 0L;
    measurementDuration = 1 * 60;
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
      correlation_result_server_ip = properties.getProperty("IP");
      if (correlation_result_server_ip == null) {
        correlation_result_server_ip = DEFAULT_CORRELATION_RESULT_SERVER_IP;
        logger.warn("IP property not found! Using default: " + correlation_result_server_ip);
      } else {
        logger.info("Using external-service IP: " + correlation_result_server_ip);
      }

      correlation_result_server_port = Integer.parseInt(properties.getProperty("PORT"));
      if (correlation_result_server_port == null) {
        correlation_result_server_port = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
        logger.warn("PORT property not found! Using default: " + correlation_result_server_port);
      } else {
        logger.info("Using external-service PORT: " + correlation_result_server_port);
      }

    } catch (IOException ioex) {
      ioex.printStackTrace();
    } finally {
      correlation_result_server_ip = DEFAULT_CORRELATION_RESULT_SERVER_IP;
      correlation_result_server_port = DEFAULT_CORRELATION_RESULT_SERVER_PORT;
      logger.warn("external-service.properties file not found under " + externalServicePath
                  + ". Using default IP: " + correlation_result_server_ip
                  + " and PORT: " + correlation_result_server_port);
      
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
  public void postDataPairwiseFinancial(IPriorityDataSinkPairwiseFinancialInput data) {
    if (terminating) return;
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, IPriorityDataSinkPairwiseFinancialInput data) {
    if (terminating) return;
    monitorMe();
    StringBuilder sb = new StringBuilder();
    sb.append("f,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getId0()).append(",")
        .append(data.getId1()).append(",")
        .append(data.getDate()).append(",")
        .append(new DecimalFormat("0.######")
                    .format(data.getValue()));
    sendStr(sb.toString());
  }

  @Override
  public void postDataAnalyzedStream(IPriorityDataSinkAnalyzedStreamInput data) {
    if (terminating) return;
    emit(-1, data);
  }

  @Override
  public void emit(int ticket,
                   IPriorityDataSinkAnalyzedStreamInput data) {
    if (terminating) return;
    StringBuilder sb = new StringBuilder();
    sb.append("w,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getSymbolId()).append(",")
        .append(dateFormat.format(data.getTimestamp())).append(",")
        .append(data.getValue());

    sendStr(sb.toString());
  }

  private void sendStr(String str) {
    try {
      writer.println(str);
      if (writer.checkError()) {
        throw new Exception("Error writing");
      }
    } catch (Exception e) {
      try {
        logger.error("Error. Disconnected from results server. Reconnecting...");
        connectToResultsServer();
        writer.println(str);
        if (writer.checkError()) {
          throw new IOException("error writing");
        }
      } catch (IOException e1) {
        logger.error(e1.getMessage(), e1);
      }
    }
  }

  private void connectToResultsServer() throws IOException {
    socket = new Socket(correlation_result_server_ip, correlation_result_server_port);
    writer = new PrintWriter(socket.getOutputStream(), true);
  }

  @Override
  public void connect() throws DefaultModeException {
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
  public void disconnect() throws DefaultModeException {
    terminating = true;
    try {
      writer.close();
      socket.close();
    } catch (IOException e) {
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

  private void monitorMe() {
    if (financialMonitoringTimestamp == 0) {
      financialMonitoringTimestamp = System.currentTimeMillis();
      ++financialThroughput;
    } else {
      long now = System.currentTimeMillis();
      if (now - financialMonitoringTimestamp < measurementDuration * 1000) {
        ++financialThroughput;
      } else {
        logger.info("Pipeline financial output throughput: "
                    + ((double) financialThroughput / (double) measurementDuration)
                    + " tuples/sec");
        financialMonitoringTimestamp = now;
        financialThroughput = 1;
      }
    }
  }
}
