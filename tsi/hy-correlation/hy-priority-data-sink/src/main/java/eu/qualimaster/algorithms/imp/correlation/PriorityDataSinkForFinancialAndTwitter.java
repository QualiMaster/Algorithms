package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.data.inf.IPriorityDataSink;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.sinks.IDataSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  private String correlationResultServerIp = "";
  private Integer correlationResultServerPort = -1;
  private Logger logger = LoggerFactory.getLogger(PriorityDataSinkForFinancialAndTwitter.class);
  private Socket socket;
  private PrintWriter writer;
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
  private long financialMonitoringTimestamp;
  private long financialThroughput;
  private int measurementDuration;  // seconds
  private boolean terminating;

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

  @Override
  public void postDataPairwiseFinancial(IPriorityDataSinkPairwiseFinancialInput data) {
    if (terminating) {
      return;
    }
    emit(-1, data);
  }

  @Override
  public void emit(int ticket, IPriorityDataSinkPairwiseFinancialInput data) {
    if (terminating) {
      return;
    }
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
    if (terminating) {
      return;
    }
    emit(-1, data);
  }

  @Override
  public void emit(int ticket,
                   IPriorityDataSinkAnalyzedStreamInput data) {
    if (terminating) {
      return;
    }
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
    socket = new Socket(correlationResultServerIp, correlationResultServerPort);
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
