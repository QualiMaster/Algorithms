package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons.Receiver;
import eu.qualimaster.events.EventManager;
import eu.qualimaster.families.imp.FCorrelationFinancial;
import eu.qualimaster.monitoring.events.HardwareAliveEvent;

import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Created by Apostolos Nydriotis on 12/18/14.
 */
public class HardwareConnectionSpout extends BaseRichSpout {

  private static final String OLYNTHOS_IP = "147.27.39.12";
  private static final String VERGINA_IP = "147.27.39.13";
  private static final int RECEIVER_PORT = 2401;
  private SpoutOutputCollector collector;
  private Receiver hardwareConnection;
  private Boolean isFinancial;
  private String typeFlag;
  private String streamId;
  private String host;

  public HardwareConnectionSpout(Boolean isFinancial, String streamId) {
    this.isFinancial = isFinancial;
    this.streamId = streamId;
    if (isFinancial) {
      typeFlag = "f";
    } else {
      typeFlag = "w";
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(streamId, false, new Fields("correlation"));
  }

  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {

    collector = spoutOutputCollector;
    if (isFinancial) {
      host = VERGINA_IP;  // financial data comes from vergina FPGA
    } else {
      host = OLYNTHOS_IP;  // twitter data comes from olynthos FPGA
    }

//    try {
//      hardwareConnection = new Receiver(host, RECEIVER_PORT);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

  public void nextTuple() {
//      try {
//        while (hardwareConnection == null) {
//          hardwareConnection = new Receiver(host, RECEIVER_PORT);
//        }
//        String receivedData = hardwareConnection.receiveData();
//        if (receivedData != null) {
//          if (isFinancial) {
//            EventManager.send(new HardwareAliveEvent("financial"));
//          } else {
//            EventManager.send(new HardwareAliveEvent("sentiment"));
//          }
//          FCorrelationFinancial.IFCorrelationFinancialPairwiseFinancialOutput
//              ifCorrelationFinancialOutput =
//              new FCorrelationFinancial.IFCorrelationFinancialPairwiseFinancialOutput();
//          ifCorrelationFinancialOutput.setPairwiseCorrelationFinancial(
//              typeFlag + "," + hardwareConnection.receiveData());
//          collector.emit(streamId, new Values(ifCorrelationFinancialOutput));
//        }
//      } catch (IOException e) {
//        e.printStackTrace();
//        try {
//          hardwareConnection = new Receiver(host, RECEIVER_PORT);
//        } catch (IOException e1) {
//          e1.printStackTrace();
//        }
//      }
    Utils.sleep(1000);
  }
}
