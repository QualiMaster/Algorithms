package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons.Transmitter;
import eu.qualimaster.common.signal.BaseSignalBolt;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.ValueFormatException;
import eu.qualimaster.families.inf.IFCorrelationFinancial;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Created by Apostolos Nydriotis on 12/18/14.
 */
public class HardwareConnectionBolt extends BaseSignalBolt {

  private static final String OLYNTHOS_IP = "147.27.39.12";
  private static final String VERGINA_IP = "147.27.39.13";
  private static final int FINANCIAL_TRANSMITTER_PORT = 2400;
  private static final int WEB_TRANSMITTER_PORT = 2402;
  OutputCollector collector;
  Transmitter hardwareConnection;
  int windowSize;
  Boolean isFinancial;
  String host;
  int transmitterPort;

  final static org.apache.log4j.Logger logger =
      org.apache.log4j.Logger.getLogger(HardwareConnectionBolt.class);

  public HardwareConnectionBolt(String name, String namespace, Boolean isFinancial) {
    super(name, namespace);
    this.isFinancial = isFinancial;
  }

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    windowSize = ((Long) map.get("windowSize")).intValue();
    if (isFinancial) {
      host = OLYNTHOS_IP;  // financial data goes to olynthos FPGA
      transmitterPort = FINANCIAL_TRANSMITTER_PORT;
    } else {
      host = VERGINA_IP;  // twitter data goes to vergina FPGA
      transmitterPort = WEB_TRANSMITTER_PORT;
    }

    // Both twitter & financial now run on OLYNTHOS, to different ports
    host = OLYNTHOS_IP;

    try {
      logger.info("openning connection");
//      hardwareConnection = new Transmitter(host, transmitterPort);
      logger.info("connection opened");
    } catch (Exception e) {
      logger.error(e);
    }
  }

  public void execute(Tuple tuple) {
//    try {
//      if (tuple.getSourceStreamId().equals("resetWindowStream")) {  // reset window
//        hardwareConnection.sendResultRequest();
//        collector.ack(tuple);
//
//      } else if (tuple.getValue(
//          0) instanceof IFCorrelationFinancial.IIFCorrelationFinancialSymbolListInput) {
//        List<String> allSymbols =
//            ((IFCorrelationFinancial.IIFCorrelationFinancialSymbolListInput) tuple
//                .getValue(0)).getAllSymbols();
//        // numberOfSymbols windowSizeInSecs step
//        hardwareConnection.sendConfiguration(
//            String.valueOf(allSymbols.size()) + " " + String.valueOf(windowSize));
//        collector.ack(tuple);
//
//      } else if (tuple.getValue(
//          0) instanceof IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput) {
//        IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput input =
//            (IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput) tuple
//                .getValue(0);
//        hardwareConnection.sendData(
//            input.getSymbolId() + "," + input.getTimestamp() + "," + input.getValue());
//        collector.ack(tuple);
//      }
//    } catch (Exception e) {
//      logger.error("Error sending", e);
//      collector.ack(tuple);  // TODO(ap0n): This should be fail
//      try {
//        hardwareConnection = new Transmitter(host, transmitterPort);
//      } catch (Exception e1) {
//        logger.error("Error reopening connection", e1);
//      }
//    }
//    collector.ack(tuple);
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public void notifyParameterChange(ParameterChangeSignal signal) {
    try {
      for (int i = 0; i < signal.getChangeCount(); i++) {
        ParameterChange parameterChange = signal.getChange(i);
        switch (parameterChange.getName()) {
          case "windowSize": {
            windowSize = parameterChange.getIntValue() * 1000;
            logger.info("Changed windowSize parameter to: " + windowSize);
            break;
          }
          default: {
            continue;
          }
        }
      }
    } catch (ValueFormatException e) {
      e.printStackTrace();
    }
  }
}
