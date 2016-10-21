package eu.qualimaster.dynamicgraph.bolts;

import eu.qualimaster.dynamicgraph.core.Helper;
import eu.qualimaster.families.inf.IFDynamicHubComputation;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Nikolaos Pavlakis on 2/5/16.
 */
public class EntryBolt extends BaseRichBolt {

  private static final Logger logger = Logger.getLogger(EntryBolt.class);

  private OutputCollector collector;
  private List<Integer> taskIds;
  private String prefix;

  public EntryBolt(String prefix) {
    this.prefix = prefix;
  }

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    taskIds = topologyContext.getComponentTasks(prefix + "ProcessingBolt");
  }

  public void execute(Tuple tuple) {

    IFDynamicHubComputation.IIFDynamicHubComputationEdgeStreamInput input =
        (IFDynamicHubComputation.IIFDynamicHubComputationEdgeStreamInput) tuple.getValue(0);
    String edge = input.getEdge();

    if (edge == null) {
      collector.ack(tuple);
      return;
    }

    // Got new edge. Need to get node ids and forward the edge. Assuming bidirectional.
    // E.g. 176,192,02/15/2016,12:52:50,1
    String[] parts = edge.split(",");
    if (parts.length == 5) {
      int nodeU = Integer.parseInt(parts[0]);
      int nodeV = Integer.parseInt(parts[1]);
      if (parts[4].equals("1")) { // Edge addition
        emitBothForAdd(nodeU, nodeV, tuple);
      } else { // Edge deletion
        emitBothForDelete(nodeU, nodeV, tuple);
      }
    } else {
      logger.error("Unknown message received.");
    }
    collector.ack(tuple);
  }

  private void emitBothForAdd(int nodeU, int nodeV, Tuple tuple) {
    emitForAdd(nodeU, nodeV, tuple);
    emitForAdd(nodeV, nodeU, tuple);
  }

  private void emitForAdd(int nodeU, int nodeV, Tuple tuple) {
    collector.emitDirect(taskIds.get(Helper.nextServerId(nodeV, taskIds.size())),
                         "SecondNodeStream", tuple, new Values(nodeV));
    collector.emitDirect(taskIds.get(Helper.nextServerId(nodeU, taskIds.size())),
                         "FirstNodeStream", new Values(nodeU, nodeV));
  }

  private void emitBothForDelete(int nodeU, int nodeV, Tuple tuple) {
    collector.emitDirect(taskIds.get(Helper.nextServerId(nodeU, taskIds.size())),
                         "DeleteEdgeStream", new Values(nodeU, nodeV));
    collector.emitDirect(taskIds.get(Helper.nextServerId(nodeV, taskIds.size())),
                         "DeleteEdgeStream", tuple, new Values(nodeV, nodeU));
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("FirstNodeStream", new Fields("FirstNode", "SecondNode"));
    outputFieldsDeclarer.declareStream("SecondNodeStream", new Fields("SecondNode"));
    outputFieldsDeclarer.declareStream("DeleteEdgeStream", new Fields("FirstNode", "SecondNode"));
  }
}
