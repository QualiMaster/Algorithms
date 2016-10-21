package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractFinancialSubTopology;
import eu.qualimaster.algorithms.imp.correlation.ResetWindowSpout;
import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;
import eu.qualimaster.common.signal.ParameterChangeSignal;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by Apostolos Nydriotis on 12/18/14.
 */
public class TopoHardwareCorrelationFinancial extends AbstractFinancialSubTopology
    implements ITopologyCreate {

  public TopoHardwareCorrelationFinancial() {
    String prefix = "TopoHardwareCorrelationFinancial";
    windowSizeHandlerExecutorNamespace = "PriorityPip";
    windowSizeHandlerExecutorName = prefix + "transmitterBolt";
  }

  public SubTopologyOutput createSubTopology(TopologyBuilder topologyBuilder, Config config,
                                             String prefix, String input, String streamId) {

    topologyBuilder.setSpout(prefix + "hardwareConnectionSpout",
                             new HardwareConnectionSpout(true, streamId), 1);
//    topologyBuilder.setSpout(activationHandlerExecutorName,
//                             new ResetWindowSpout(activationHandlerExecutorName,
//                                                  activationHandlerNamespace,
//                                                  false), 1);
    topologyBuilder.setBolt(windowSizeHandlerExecutorName,
                            new HardwareConnectionBolt(windowSizeHandlerExecutorName,
                                                       windowSizeHandlerExecutorNamespace,
                                                       true), 1).shuffleGrouping(input, streamId);
//        .allGrouping(prefix + "ResetWindowSpout", "resetWindowStream");

    return new SubTopologyOutput(prefix + "hardwareConnectionSpout", streamId, 1, 1);
  }

  public void calculate(
      IIFCorrelationFinancialPreprocessedStreamInput iifCorrelationFinancialPreprocessedStreamInput,
      IIFCorrelationFinancialPairwiseFinancialOutput iifCorrelationFinancialPairwiseFinancialOutput) {
    // do nothing here
  }

  public void calculate(
      IIFCorrelationFinancialSymbolListInput iifCorrelationFinancialSymbolListInput,
      IIFCorrelationFinancialPairwiseFinancialOutput iifCorrelationFinancialPairwiseFinancialOutput) {
    // do nothing here
  }

//  @Override
//  public void setParameterWindowSize(int i) {
//
//    logger.info("setParameterWindowSize");
//
//    try {
//      logger.info("sending new windowSize signal " + i + "!");
//      ParameterChangeSignal parameterChangeSignal =
//          new ParameterChangeSignal(windowSizeHandlerExecutorNamespace,
//                                    windowSizeHandlerExecutorName,
//                                    "windowSize", i);
//      parameterChangeSignal.sendSignal();
//
//    } catch (Exception e) {
//      logger.error("Signal not sent!");
//      e.printStackTrace();
//    }
//  }
}