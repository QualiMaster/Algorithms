package eu.qualimaster.dynamicgraph.bolts;

import eu.qualimaster.dynamicgraph.core.Consts;
import eu.qualimaster.dynamicgraph.core.FipWalk;
import eu.qualimaster.dynamicgraph.core.Graph;
import eu.qualimaster.dynamicgraph.core.Helper;
import eu.qualimaster.dynamicgraph.core.RandomWalk;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

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
 * Created by Nikolaos Pavlakis on 12/23/15.
 */
public class DSPMBolt extends BaseRichBolt {

  private static final Logger logger = Logger.getLogger(DSPMBolt.class);
  private OutputCollector collector;
  private Graph graph;
  private int thisTaskIndex;
  private ObjectArrayList<RandomWalk> pendingRandomWalks;
  private ObjectArrayList<FipWalk> pendingFipWalks;
  private List<Integer> taskIds;

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    thisTaskIndex = topologyContext.getThisTaskIndex();
    taskIds = topologyContext.getComponentTasks(topologyContext.getThisComponentId());
    graph = new Graph(thisTaskIndex, taskIds.size());
    pendingRandomWalks = new ObjectArrayList<RandomWalk>();
    pendingFipWalks = new ObjectArrayList<FipWalk>();
  }

  public void execute(Tuple tuple) {
    if (tuple.getSourceStreamId().equals("FirstNodeStream")) {
      graph.processNewEdgeFirst(tuple.getIntegerByField("FirstNode"),
                                tuple.getIntegerByField("SecondNode"));
    } else if (tuple.getSourceStreamId().equals("SecondNodeStream")) {
      graph.processNewEdgeSecond(tuple.getIntegerByField("SecondNode"));
    } else if (tuple.getSourceStreamId().equals("DeleteEdgeStream")) {
      graph.removeEdge(tuple.getIntegerByField("FirstNode"),
                       tuple.getIntegerByField("SecondNode"));
    } else if (tuple.getSourceStreamId().equals("WalkStream")) {
      pendingRandomWalks.add(new RandomWalk(tuple.getIntegerByField("NodeId"),
                                            tuple.getIntegerByField("NumOfWalks"),
                                            tuple.getIntegerByField("EdgeNotToTraverse")));
      executePendingRandomWalks();
    } else if (tuple.getSourceStreamId().equals("FipWalkStream")) {
      FipWalk fw = new FipWalk(tuple.getIntegerByField("NodeId"), tuple.getIntegerByField("WalkId"),
                               tuple.getIntegerByField("CurrentPosition"));
      pendingFipWalks.add(fw);
      executePendingFipWalks();
    } else {
      logger.error("Unknown stream received.");
      collector.ack(tuple);
      return;
    }

    // Forward random walks intended for other servers
    forwardRandomWalks(tuple);

    // Forward fip walks intended for other servers
    forwardFipWalks(tuple);

    // Forward all the new visit counts
    forwardVisitsChanged();

    if (Consts.ACK_ENABLED) {
      collector.ack(tuple);
    }
  }

  private void forwardVisitsChanged() {
    for (Map.Entry<Integer, Integer> e : graph.getVisitsChanged().entrySet()) {
      collector.emit("VisitCountStream", new Values(e.getKey(), e.getValue()));
    }
    graph.clearVisitsChanged();
  }

  private void executePendingRandomWalks() {
    for (int i = 0; i < pendingRandomWalks.size(); i++) {
      RandomWalk rw = pendingRandomWalks.get(i);
      if (graph.containsNode(rw.getNodeId())) {
        graph.randomWalk(rw.getNodeId(), rw.getNumOfWalks(), rw.getEdgeNotToTraverse());
        pendingRandomWalks.remove(rw);
      }
    }
  }

  private void executePendingFipWalks() {
    for (int i = 0; i < pendingFipWalks.size(); i++) {
      FipWalk fw = pendingFipWalks.get(i);
      if (graph.containsNode(fw.getNodeId())) {
        graph.fipWalk(fw.getNodeId(), fw.getWalkId(), fw.getCurrentPosition());
        pendingFipWalks.remove(fw);
      }
    }
  }

  /**
   * Forwards all pending random walks to other servers
   *
   * @param tuple Anchoring tuple
   */
  private void forwardRandomWalks(Tuple tuple) {
    ObjectArrayList<RandomWalk> walksForOtherServers = graph.getWalksForOtherServers();
    // Emit all random walks to other servers
    for (int i = 0; i < walksForOtherServers.size(); i++) {
      RandomWalk tempRW = walksForOtherServers.get(i);
      if (Consts.ACK_ENABLED) {
        collector.emitDirect(taskIds.get(Helper.nextServerId(tempRW.getNodeId(), taskIds.size())),
                             "WalkStream",
                             tuple,
                             new Values(tempRW.getNodeId(), tempRW.getNumOfWalks(),
                                        tempRW.getEdgeNotToTraverse()));
      } else {
        collector.emitDirect(taskIds.get(Helper.nextServerId(tempRW.getNodeId(), taskIds.size())),
                             "WalkStream",
                             new Values(tempRW.getNodeId(), tempRW.getNumOfWalks(),
                                        tempRW.getEdgeNotToTraverse()));
      }
    }
    graph.clearWalksForOtherServers();
  }

  /**
   * Forwards all pending fip walks to other servers
   *
   * @param tuple Anchoring tuple
   */
  private void forwardFipWalks(Tuple tuple) {
    ObjectArrayList<FipWalk> fipWalks = graph.getFipWalksForOtherServer();
    // Emit all fip walks to other servers
    for (int i = 0; i < fipWalks.size(); i++) {
      if (Consts.ACK_ENABLED) {
        collector.emitDirect(taskIds.get(
                                 Helper.nextServerId(fipWalks.get(i).getNodeId(), taskIds.size())),
                             "FipWalkStream", tuple,
                             new Values(fipWalks.get(i).getNodeId(), fipWalks.get(i).getWalkId(),
                                        fipWalks.get(i).getCurrentPosition()));
      } else {
        collector.emitDirect(taskIds.get(
                                 Helper.nextServerId(fipWalks.get(i).getNodeId(), taskIds.size())),
                             "FipWalkStream",
                             new Values(fipWalks.get(i).getNodeId(), fipWalks.get(i).getWalkId(),
                                        fipWalks.get(i).getCurrentPosition()));
      }
    }
    graph.clearFipWalksForOtherServers();
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer
        .declareStream("WalkStream", new Fields("NodeId", "NumOfWalks", "EdgeNotToTraverse"));
    outputFieldsDeclarer
        .declareStream("FipWalkStream",
                       new Fields("NodeId", "WalkId", "CurrentPosition"));
    outputFieldsDeclarer.declareStream("VisitCountStream", new Fields("streamId", "visitCount"));
  }
}
