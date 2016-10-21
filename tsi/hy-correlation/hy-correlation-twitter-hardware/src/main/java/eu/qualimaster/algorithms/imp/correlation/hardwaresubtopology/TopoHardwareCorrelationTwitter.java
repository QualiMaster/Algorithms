package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractTwitterSubTopology;
import eu.qualimaster.algorithms.imp.correlation.ResetWindowSpout;
import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;
import eu.qualimaster.common.signal.ParameterChangeSignal;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by Apostolos Nydriotis on 12/18/14.
 */
public class TopoHardwareCorrelationTwitter extends AbstractTwitterSubTopology
    implements ITopologyCreate {

  public TopoHardwareCorrelationTwitter() {
    String prefix = "TopoHardwareCorrelationTwitter";
    activationHandlerNamespace = "PriorityPip";
    activationHandlerExecutorName = prefix + "ResetWindowSpout";
    windowSizeHandlerExecutorNamespace = activationHandlerNamespace;
    windowSizeHandlerExecutorName = prefix + "transmitterBolt";
  }

  public SubTopologyOutput createSubTopology(TopologyBuilder topologyBuilder, Config config,
                                             String prefix, String input, String streamId) {

    topologyBuilder.setSpout(prefix + "hardwareConnectionSpout",
                             new HardwareConnectionSpout(false, streamId), 1);
    topologyBuilder.setSpout(activationHandlerExecutorName,
                             new ResetWindowSpout(activationHandlerExecutorName,
                                                  activationHandlerNamespace,
                                                  false), 1);
    topologyBuilder.setBolt(windowSizeHandlerExecutorName,
                            new HardwareConnectionBolt(windowSizeHandlerExecutorName,
                                                       windowSizeHandlerExecutorNamespace,
                                                       false), 1).shuffleGrouping(input, streamId)
        .allGrouping(prefix + "ResetWindowSpout", "resetWindowStream");

    return new SubTopologyOutput(prefix + "hardwareConnectionSpout", streamId, 1, 1);
  }

  public void calculate(
      IIFCorrelationTwitterAnalyzedStreamInput iifCorrelationTwitterAnalyzedStreamInput,
      IIFCorrelationTwitterPairwiseTwitterOutput iifCorrelationTwitterPairwiseTwitterOutput) {
    // to nothing here
  }

  public void calculate(IIFCorrelationTwitterSymbolListInput iifCorrelationTwitterSymbolListInput,
                        IIFCorrelationTwitterPairwiseTwitterOutput iifCorrelationTwitterPairwiseTwitterOutput) {
    // to nothing here
  }

  @Override
  public void setParameterWindowSize(int i) {

    logger.info("setParameterWindowSize");

    try {
      logger.info("sending new windowSize signal " + i + "!");
      ParameterChangeSignal parameterChangeSignal =
          new ParameterChangeSignal(windowSizeHandlerExecutorNamespace,
                                    windowSizeHandlerExecutorName,
                                    "windowSize", i);
      parameterChangeSignal.sendSignal();

      parameterChangeSignal =
          new ParameterChangeSignal(activationHandlerNamespace,
                                    activationHandlerExecutorName,
                                    "windowSize", i);
      parameterChangeSignal.sendSignal();
    } catch (Exception e) {
      logger.error("Signal not sent!");
      e.printStackTrace();
    }
  }
}
