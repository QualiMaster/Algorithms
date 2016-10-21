package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.common.signal.BaseSignalSpout;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.ValueFormatException;

import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Created by Apostolos Nydriotis on 12/11/14.
 */
public class ResetWindowSpout extends BaseSignalSpout {

  final static Logger logger = Logger.getLogger(ResetWindowSpout.class);

  SpoutOutputCollector collector;
  long windowStart;
  int windowSize;  // window size in ms
  int windowAdvance;  // window advance in ms
  boolean isActive;  // True if the SW subTopology is active

  public ResetWindowSpout(String name, String namespace, Boolean isActive) {
    super(name, namespace);
    this.isActive = isActive;
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("resetWindowStream", new Fields("windowStart", "windowEnd" ,
                                                                       "now"));
  }

  @Override
  public void open(Map config, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {

    super.open(config, topologyContext, spoutOutputCollector);

    collector = spoutOutputCollector;
    windowStart = new Date().getTime();
    windowSize = ((Long) config.get("windowSize")).intValue() * 1000;
    windowAdvance = ((Long) config.get("windowAdvance")).intValue() * 1000;
  }

  public void nextTuple() {
//    Utils.sleep(100);  //  Check if window has ended every .1 secs
//    if (isActive) {
//      Date now = new Date();
//      if (now.getTime() - windowStart > windowSize) {
//        collector
//            .emit("resetWindowStream", new Values(windowStart, windowStart + windowSize, now));
//        windowStart += windowAdvance;
//      }
//    }
    Utils.sleep(1000);
  }

  public void activateComponent() {
    isActive = true;
    windowStart = new Date().getTime();
  }

  public void deactivateComponent() {
    isActive = false;
  }

  @Override
  public void notifyParameterChange(ParameterChangeSignal signal) {
    logger.info("in notifyParameterChange");
    try {
      for (int i = 0; i < signal.getChangeCount(); i++) {
        ParameterChange parameterChange = signal.getChange(i);
        logger.info("Got parameterChange: " + parameterChange.getName());
        switch (parameterChange.getName()) {
          case "activate" : {
            logger.info("activating. windowSize = " + windowSize);
            activateComponent();
            break;
          }
          case "passivate" : {
            logger.info("passivating");
            deactivateComponent();
            break;
          }
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
