package eu.qualimaster.algorithms.imp;

import eu.qualimaster.data.inf.ITransferSink;
import eu.qualimaster.dataManagement.sinks.IDataSink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final String PROPERTIES_PATH = "/var/nfs/qm/tsi/external-service.properties";
  private static String CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static int CORRELATION_RESULT_SERVER_PORT = 8888;
  private static String REPLAY_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static int REPLAY_RESULT_SERVER_PORT = 8888;
  private Logger logger = LoggerFactory.getLogger(TransferSink.class);
  private Socket socket, replaySocket;
  private PrintWriter writer, replayWriter;

  private boolean terminating;

  public TransferSink() {
    readPropertiesFile();
    terminating = false;
  }

  private void readPropertiesFile() {
    Properties properties = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(PROPERTIES_PATH);
      properties.load(inputStream);
      CORRELATION_RESULT_SERVER_IP = properties.getProperty("IP");
      CORRELATION_RESULT_SERVER_PORT = Integer.parseInt(properties.getProperty("PORT"));
      REPLAY_RESULT_SERVER_IP = properties.getProperty("REPLAY_IP");
      REPLAY_RESULT_SERVER_PORT = Integer.parseInt(properties.getProperty("REPLAY_PORT"));
    } catch (IOException ioex) {
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

  @Override public void postDataPairwiseFinancial(ITransferSinkPairwiseFinancialInput data) {
    sendToServer(-1, data, false);
  }

  @Override public void emit(int ticket, ITransferSinkPairwiseFinancialInput data) {
    sendToServer(ticket, data, true);
  }

  private void sendToServer(int ticket, ITransferSinkPairwiseFinancialInput data, boolean isReplay) {
    if (terminating)
      return;
    StringBuilder sb = new StringBuilder();
    sb.append("te,");
    if (ticket != -1) {
      sb.append(ticket).append(",");
    }
    sb.append(data.getId0()).append(",").append(data.getId1()).append(",").append(data.getDate()).append(",")
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
    replaySocket = new Socket(REPLAY_RESULT_SERVER_IP, REPLAY_RESULT_SERVER_PORT);
    replayWriter = new PrintWriter(replaySocket.getOutputStream(), true);
  }

  private void connectToNormalServer() throws IOException {
    socket = new Socket(CORRELATION_RESULT_SERVER_IP, CORRELATION_RESULT_SERVER_PORT);
    writer = new PrintWriter(socket.getOutputStream(), true);
  }

  @Override public void connect() {
    try {
      terminating = false;
      connectToResultsServer();
    } catch (IOException e) {
      // Ignore exception, means could not connect to external service. Continue
      e.printStackTrace();
      //      throw new DefaultModeException(e.getMessage());
    }
  }

  @Override public void disconnect() {
    terminating = true;
    writer.close();
    replayWriter.close();
    try {
      socket.close();
      replaySocket.close();
    } catch (IOException e) {
    }
  }

  @Override public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  @Override public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
