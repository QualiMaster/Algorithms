package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractFinancialSubTopology;
import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;

import java.util.LinkedList;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by Apostolos Nydriotis on 12/11/14.
 */
public class TopoSoftwareCorrelationFinancial extends AbstractFinancialSubTopology
    implements ITopologyCreate {

  private String prefix;
  private String namespace;
  private String mapperName;
  private String hayashiYoshidaName;

  public TopoSoftwareCorrelationFinancial() {

    prefix = "TopoSoftwareCorrelationFinancial";
    namespace = "PriorityPip";
    mapperName = prefix + "MapperBolt";
    hayashiYoshidaName = prefix + "HayashiYoshidaBolt";

    windowSizeHandlerExecutorNamespace = namespace;
    windowSizeHandlerExecutorName = mapperName;

    terminationHandlersNames = new LinkedList<>();
    terminationHandlersNames.add(mapperName);
    terminationHandlersNames.add(hayashiYoshidaName);
    terminationHandlersNameSpaces = new LinkedList<>();
    terminationHandlersNameSpaces.add(namespace);
    terminationHandlersNameSpaces.add(namespace);
  }

  @Override
  public SubTopologyOutput createSubTopology(TopologyBuilder topologyBuilder, Config config,
                                             String prefix, String input, String streamId) {
//    topologyBuilder
//        .setSpout(activationHandlerExecutorName, new ResetWindowSpout(activationHandlerExecutorName,
//                                                                      activationHandlerNamespace,
//                                                                      true), 1);
    topologyBuilder.setBolt(prefix + "MapperBolt", new MapperBolt(mapperName, namespace, prefix), 1)
        .shuffleGrouping(input, streamId);

    topologyBuilder.setBolt(hayashiYoshidaName,
                            new HayashiYoshidaBolt(hayashiYoshidaName, namespace, true, streamId),
                            13)
        .directGrouping(mapperName, "symbolsStream")
        .directGrouping(mapperName, "configurationStream")
        .allGrouping(mapperName, "resetWindowStream");
//        .shuffleGrouping(prefix + "MapperBolt", "allSymbolsStringStream");

    return new SubTopologyOutput(hayashiYoshidaName, streamId, 13, 13);
  }

  @Override
  public void calculate(
      IIFCorrelationFinancialPreprocessedStreamInput iifCorrelationFinancialPreprocessedStreamInput,
      IIFCorrelationFinancialPairwiseFinancialOutput iifCorrelationFinancialPairwiseFinancialOutput) {
    // Do nothing here
  }

  @Override
  public void calculate(
      IIFCorrelationFinancialSymbolListInput iifCorrelationFinancialSymbolListInput,
      IIFCorrelationFinancialPairwiseFinancialOutput iifCorrelationFinancialPairwiseFinancialOutput) {
    // Do nothing here
  }
}
