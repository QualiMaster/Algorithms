//package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;
//
//import eu.qualimaster.algorithms.imp.correlation.AbstractTwitterSubTopology;
//import eu.qualimaster.algorithms.imp.correlation.ResetWindowSpout;
//import eu.qualimaster.base.algorithm.ITopologyCreate;
//import eu.qualimaster.base.algorithm.SubTopologyOutput;
//
//import backtype.storm.Config;
//import backtype.storm.topology.TopologyBuilder;
//
///**
// * Created by Apostolos Nydriotis on 12/11/14.
// */
//public class TopoSoftwareCorrelationTwitter extends AbstractTwitterSubTopology
//    implements ITopologyCreate {
//
//  public TopoSoftwareCorrelationTwitter() {
//    String prefix = "TopoSoftwareCorrelationTwitter";
//    activationHandlerNamespace = "PriorityPip";
//    activationHandlerExecutorName = prefix + "ResetWindowSpout";
//    windowSizeHandlerExecutorNamespace = activationHandlerNamespace;
//    windowSizeHandlerExecutorName = activationHandlerExecutorName;
//  }
//
//  @Override
//  public SubTopologyOutput createSubTopology(TopologyBuilder topologyBuilder, Config config,
//                                             String prefix, String input, String streamId) {
////    topologyBuilder
////        .setSpout(activationHandlerExecutorName, new ResetWindowSpout(activationHandlerExecutorName,
////                                                                      activationHandlerNamespace,
////                                                                     true), 1);
//    topologyBuilder.setBolt(prefix + "MapperBolt", new MapperBolt(prefix + "MapperBolt",
//                                                                  activationHandlerExecutorName,
//                                                                  prefix), 1)
//        .shuffleGrouping(input, streamId);
//
//    topologyBuilder.setBolt(prefix + "HayashiYoshidaBolt", new HayashiYoshidaBolt(false, streamId),
//                            1)
//        .directGrouping(prefix + "MapperBolt", "symbolsStream")
//        .directGrouping(prefix + "MapperBolt", "configurationStream")
//        .allGrouping(prefix + "ResetWindowSpout", "resetWindowStream");
////        .shuffleGrouping(prefix + "MapperBolt", "allSymbolsStringStream");
//
//    return new SubTopologyOutput(prefix + "HayashiYoshidaBolt", streamId, 1, 1);
//  }
//
//  @Override
//  public void calculate(
//      IIFCorrelationTwitterAnalyzedStreamInput iifCorrelationTwitterAnalyzedStreamInput,
//      IIFCorrelationTwitterPairwiseTwitterOutput iifCorrelationTwitterPairwiseTwitterOutput) {
//    // do nothing here
//  }
//
//  @Override
//  public void calculate(
//      IIFCorrelationTwitterSymbolListInput iifCorrelationTwitterSymbolListInput,
//      IIFCorrelationTwitterPairwiseTwitterOutput iifCorrelationTwitterPairwiseTwitterOutput) {
//    // do nothing here
//  }
//}
