package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.data.inf.IMI_data_Sink;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Nikolaos Pavlakis on 1/16/15.
 */
public class MIDataSinkForFinancial implements IMI_data_Sink {

//  private static final String CORRELATION_RESULT_SERVER_IP = "snf-618466.vm.okeanos.grnet.gr";
//  private static final String CORRELATION_RESULT_SERVER_IP = "147.27.14.117";
//  private static final String CORRELATION_RESULT_SERVER_IP = "clu01.softnet.tuc.gr";
  private static final int CORRELATION_RESULT_SERVER_PORT = 8888;
  private Logger logger = LoggerFactory.getLogger(MIDataSinkForFinancial.class);
  private Socket socket;
  private PrintWriter writer;
  private Object writerLock;
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");

  private long financialMonitoringTimestamp;
  private long financialThroughput;
  private int measurementDuration;  // seconds

  public MIDataSinkForFinancial() {
    financialMonitoringTimestamp = 0L;
    financialThroughput = 0L;
    measurementDuration = 1 * 60;
    writerLock = new Object();
  }

  @Override
  public void postDataPairwiseFinancial(
      IMI_data_SinkPairwiseFinancialInput iMI_data_SinkPairwiseFinancialInput)
      throws DefaultModeException {

    monitorMe();

//      try {
//        writer.println(
//            "f," + iMI_data_SinkPairwiseFinancialInput.getPairwiseCorrelationFinancial());
//      } catch (Exception e) {
//        try {
//          logger.error("Error. Disconnected from results server. Reconnecting...");
//          connectToResultsServer();
//          writer.println(iMI_data_SinkPairwiseFinancialInput.getPairwiseCorrelationFinancial());
//        } catch (IOException e1) {
//          logger.error(e1.getMessage(), e1);
//          throw new DefaultModeException(e.getMessage());
//        }
//      }
  }

  @Override
  public void postDataAnalyzedStream(
      IMI_data_SinkAnalyzedStreamInput imi_data_sinkAnalyzedStreamInput) {}

//  @Override
//  public void MI_data_SinkAnalyzedStreamInput(
//      IMI_data_SinkAnalyzedStreamInput iMI_data_SinkAnalyzedStreamInput) {
//
//    String output = "w,";
//    output +=
//        iPriorityDataSinkAnalyzedStreamInput.getSymbolId() + "," + dateFormat
//            .format(iPriorityDataSinkAnalyzedStreamInput.getTimestamp()) + "," +
//        iPriorityDataSinkAnalyzedStreamInput.getValue();
//      try {
//        writer.println(output);
//      } catch (Exception e) {
//        try {
//          logger.error("Error. Disconnected from results server. Reconnecting...");
//          connectToResultsServer();
//          writer.println(output);
//        } catch (IOException e1) {
//          logger.error(e1.getMessage(), e1);
//          throw new DefaultModeException(e.getMessage());
//        }
//      }
//  }

//  @Override
//  public void postDataPairwiseTwitter(
//      IPriorityDataSinkPairwiseTwitterInput iPriorityDataSinkPairwiseTwitterInput)
//      throws DefaultModeException {

//    synchronized (writerLock) {
//      try {
//        writer.println(iPriorityDataSinkPairwiseTwitterInput.getPairwiseCorrelationTwitter());
//      } catch (Exception e) {
//        try {
//          logger.error("Error. Disconnected from results server. Reconnecting...");
//          connectToResultsServer();
//          writer.println(iPriorityDataSinkPairwiseTwitterInput.getPairwiseCorrelationTwitter());
//        } catch (IOException e1) {
//          logger.error(e1.getMessage(), e1);
//          throw new DefaultModeException(e.getMessage());
//        }
//      }
//    }
//  }

  private void connectToResultsServer() throws IOException {
//      socket = new Socket(CORRELATION_RESULT_SERVER_IP, CORRELATION_RESULT_SERVER_PORT);
//      writer = new PrintWriter(socket.getOutputStream());
  }

  @Override
  public void connect() throws DefaultModeException {
//    try {
//      connectToResultsServer();
//    } catch (IOException e) {
//      throw new DefaultModeException(e.getMessage());
//    }
  }

  @Override
  public void disconnect() throws DefaultModeException {
   // writer.close();
  //  try {
   //   socket.close();
  //  } catch (IOException e) {
  //    throw new DefaultModeException(e.getMessage());
   // }
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
      financialMonitoringTimestamp = new Date().getTime();
      ++financialThroughput;
    } else {
      long now = new Date().getTime();
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

  public static class MI_data_SinkPairwiseFinancialInput
      implements IMI_data_SinkPairwiseFinancialInput {

    String pairwiseCorrelation;

    @Override
    public String getPairwiseCorrelationFinancial() {
      return pairwiseCorrelation;
    }

    @Override
    public void setPairwiseCorrelationFinancial(String s) {
      pairwiseCorrelation = s;
    }
  }
}
