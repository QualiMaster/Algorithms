package eu.qualimaster.dynamicgraph.bolts;

import eu.qualimaster.common.signal.BaseSignalBolt;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.ValueFormatException;

import org.apache.log4j.Logger;
import eu.qualimaster.families.imp.FDynamicHubComputation;
import eu.qualimaster.families.inf.IFDynamicHubComputation;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Nikolaos Pavlakis on 2/5/16.
 */
public class HubListBolt extends BaseSignalBolt {
  private static final Logger logger = Logger.getLogger(HubListBolt.class);

  private OutputCollector collector;
  private int hubListSize;
  private PriorityQueue<StreamCount> hubList;
  private StreamCountComparator comparator;
  private String outputStream;
  private HashMap<Integer, StreamCount> hubListMap;

  public HubListBolt(String outputStream, String name, String namespace) {
    super(name, namespace);
    this.outputStream = outputStream;
  }

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
    collector = outputCollector;
    hubListSize = 10;
    comparator = new StreamCountComparator();
    hubList = new PriorityQueue<StreamCount>(hubListSize, comparator);
    hubListMap = new HashMap<Integer, StreamCount>(hubListSize);
  }

  public void execute(Tuple tuple) {
    if (tuple.getSourceStreamId().equals("VisitCountStream")) {
      int streamId = tuple.getIntegerByField("streamId");
      int visitCount = tuple.getIntegerByField("visitCount");
      StreamCount streamCount = new StreamCount(streamId, visitCount);
      addToList(streamCount);
    } else if (tuple.getSourceStreamId().equals("ForwardHubListStream")) {
      IFDynamicHubComputation.IIFDynamicHubComputationHubStreamOutput
          output =
          new FDynamicHubComputation.IFDynamicHubComputationHubStreamOutput();
      output.setHubList(getTopHubs());
      collector.emit(outputStream, tuple, new Values(output));
    }
    collector.ack(tuple);
  }

  public void notifyParameterChange(ParameterChangeSignal signal) {
    logger.info("got parameter change signal: " + signal.toString());
    try {
      for (int i = 0; i < signal.getChangeCount(); i++) {
        ParameterChange parameterChange = signal.getChange(i);
        switch (parameterChange.getName()) {
          case "hubListSize": {
            hubListSize = parameterChange.getIntValue();
            logger.info("Changed hubListSize parameter to: " + hubListSize);
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

  private String getTopHubs() {
    String topHubs = "";
    while (!hubList.isEmpty()) {
      StreamCount stream = hubList.poll();
      if (hubList.size() == hubListSize - 1) {
        topHubs = String.valueOf(stream.getStreamId());
      } else {
        topHubs = stream.getStreamId() + "," + topHubs;
      }
    }
    return topHubs;
  }

  private void addToList(StreamCount streamCount) {
    if (hubListMap.containsKey(streamCount.getStreamId())) {
      StreamCount oldItem = hubListMap.get(streamCount.getStreamId());
      hubList.remove(oldItem);
      hubListMap.remove(streamCount.getStreamId()); // Put would overwrite anyway
    }
    hubList.add(streamCount);
    hubListMap.put(streamCount.getStreamId(), streamCount);

    // Need "while" because hubListSize may change by a signal
    while (hubList.size() > hubListSize) {
      StreamCount removed = hubList.poll();
      hubListMap.remove(removed.getStreamId());
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(outputStream, new Fields("hubList"));
  }

  private class StreamCount {

    private int streamId;
    private int visitCount;

    public StreamCount(int streamId, int visitCount) {
      this.streamId = streamId;
      this.visitCount = visitCount;
    }

    public int getStreamId() {
      return streamId;
    }

    public int getVisitCount() {
      return visitCount;
    }
  }

  private class StreamCountComparator implements Comparator<StreamCount> {

    public int compare(StreamCount a, StreamCount b) {
      return a.getVisitCount() - b.getVisitCount();
    }
  }
}
