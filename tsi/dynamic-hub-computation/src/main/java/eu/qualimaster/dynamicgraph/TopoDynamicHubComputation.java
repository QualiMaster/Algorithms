package eu.qualimaster.dynamicgraph;

import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.dynamicgraph.bolts.DSPMBolt;
import eu.qualimaster.dynamicgraph.bolts.EntryBolt;
import eu.qualimaster.dynamicgraph.bolts.HubListBolt;
import eu.qualimaster.dynamicgraph.spouts.ForwardHubListSpout;
import eu.qualimaster.families.inf.IFDynamicHubComputation;
import eu.qualimaster.observables.IObservable;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by ap0n on 5/2/2016.
 */
public class TopoDynamicHubComputation implements ITopologyCreate, IFDynamicHubComputation {

  private static final Logger logger = Logger.getLogger(TopoDynamicHubComputation.class);

  public SubTopologyOutput createSubTopology(TopologyBuilder topologyBuilder, Config config,
                                             String prefix, String input, String streamId) {

    topologyBuilder.setSpout(prefix + "ForwardHubList",
                             new ForwardHubListSpout("TopoDynamicHubComputation" + "ForwardHubList",
                                                     "DynamicGraphPip"), 1);
    topologyBuilder.setBolt(prefix + "EntryBolt", new EntryBolt(prefix), 1)
        .shuffleGrouping(input, streamId);
    BoltDeclarer
        DSPM_bolt = topologyBuilder.setBolt(prefix + "ProcessingBolt", new DSPMBolt(), 10)
        .directGrouping(prefix + "EntryBolt", "FirstNodeStream")
        .directGrouping(prefix + "EntryBolt", "SecondNodeStream")
        .directGrouping(prefix + "EntryBolt", "DeleteEdgeStream");

    DSPM_bolt.directGrouping(prefix + "ProcessingBolt", "WalkStream")
        .directGrouping(prefix + "ProcessingBolt", "FipWalkStream");

    topologyBuilder.setBolt(prefix + "HubListBolt",
                            new HubListBolt(streamId, "TopoDynamicHubComputation" + "HubListBolt",
                                            "DynamicGraphPip"), 1)
        .shuffleGrouping(prefix + "ProcessingBolt", "VisitCountStream")
        .shuffleGrouping(prefix + "ForwardHubList", "ForwardHubListStream");

    return new SubTopologyOutput(prefix + "HubListBolt", streamId, 1, 1);
  }

  public void calculate(
      IIFDynamicHubComputationEdgeStreamInput iifDynamicHubComputationEdgeStreamInput,
      IIFDynamicHubComputationHubStreamOutput iifDynamicHubComputationHubStreamOutput) {

  }

  public void setParameterWindowSize(int i) {
    try {
      logger.info("Sending new windowSize signal " + i + "!");
      ParameterChangeSignal parameterChangeSignal =
          new ParameterChangeSignal("DynamicGraphPip",
                                    "TopoDynamicHubComputation" + "ForwardHubList",
                                    "windowSize", i);
      parameterChangeSignal.sendSignal();
    } catch (Exception e) {
      logger.error("Signal for windowSize not sent!", e);
    }
  }

  public void setParameterHubListSize(int i) {
    try {
      logger.info("Sending new hubListSize signal " + i + "!");

      ParameterChangeSignal parameterChangeSignal =
          new ParameterChangeSignal("DynamicGraphPip",
                                    "TopoDynamicHubComputation" + "HubListBolt",
                                    "hubListSize", i);
      parameterChangeSignal.sendSignal();
    } catch (Exception e) {
      logger.error("Signal for hubListSize not sent!", e);
    }
  }

  public void switchState(State state) {

  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
