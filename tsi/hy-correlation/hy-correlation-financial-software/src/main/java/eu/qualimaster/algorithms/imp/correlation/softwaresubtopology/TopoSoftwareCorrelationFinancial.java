package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractFinancialSubTopology;
import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;
import eu.qualimaster.infrastructure.PipelineOptions;

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

    topologyBuilder.setBolt(prefix + "MapperBolt", new MapperBolt(mapperName, namespace, prefix), 1)
        .shuffleGrouping(input, streamId);

    int parallelism_hint = PipelineOptions.getExecutorParallelism(config, hayashiYoshidaName, 13);

    topologyBuilder.setBolt(hayashiYoshidaName,
                            new HayashiYoshidaBolt(hayashiYoshidaName, namespace, true, streamId),
                            parallelism_hint)
        .setNumTasks(parallelism_hint)  // TODO: Should #tasks > #executors for scaling up? If yes,
                                        // how do we decide the #tasks?
        .directGrouping(mapperName, "symbolsStream")
        .directGrouping(mapperName, "configurationStream")
        .allGrouping(mapperName, "resetWindowStream");

    return new SubTopologyOutput(hayashiYoshidaName, streamId, parallelism_hint + 1,
                                 parallelism_hint);  // TODO: How do we decide the numWorkers?
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
