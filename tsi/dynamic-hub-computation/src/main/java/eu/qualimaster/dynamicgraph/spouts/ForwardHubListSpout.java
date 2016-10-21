package eu.qualimaster.dynamicgraph.spouts;

import eu.qualimaster.common.signal.BaseSignalSpout;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.ValueFormatException;

import org.apache.log4j.Logger;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Created by Nikolaos Pavlakis on 2/8/16.
 */
public class ForwardHubListSpout extends BaseSignalSpout {

  private static final Logger logger = Logger.getLogger(ForwardHubListSpout.class);

  private int windowSize; // In seconds
  private SpoutOutputCollector collector;
  private long lastSignal;

  public ForwardHubListSpout(String name, String namespace) {
    super(name, namespace);
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("ForwardHubListStream", new Fields("true"));
  }

  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    super.open(map, topologyContext, spoutOutputCollector);
    collector = spoutOutputCollector;
    windowSize = 10;
    lastSignal = System.currentTimeMillis();
  }

  public void nextTuple() {
    long now = System.currentTimeMillis();
    if (now - lastSignal < windowSize * 1000) {
      Utils.sleep(1000);
    } else {
      collector.emit("ForwardHubListStream", new Values(true));
      lastSignal = now;
    }
  }

  public void notifyParameterChange(ParameterChangeSignal signal) {
    logger.info("got parameter change signal: " + signal.toString());
    try {
      for (int i = 0; i < signal.getChangeCount(); i++) {
        ParameterChange parameterChange = signal.getChange(i);
        switch (parameterChange.getName()) {
          case "windowSize": {
            windowSize = parameterChange.getIntValue();
            logger.info("Changed windowSize parameter to: " + windowSize);
            break;
          }
          default: {
            logger.info("unknown parameter: " + parameterChange.getName());
            continue;
          }
        }
      }
    } catch (ValueFormatException e) {
      e.printStackTrace();
    }
  }
}
