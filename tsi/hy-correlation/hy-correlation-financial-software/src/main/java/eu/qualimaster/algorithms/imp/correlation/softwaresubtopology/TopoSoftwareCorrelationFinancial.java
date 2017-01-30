package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractFinancialSubTopology;
import eu.qualimaster.base.algorithm.IScalableTopology;
import eu.qualimaster.base.algorithm.ITopologyCreate;
import eu.qualimaster.base.algorithm.SubTopologyOutput;
import eu.qualimaster.infrastructure.IScalingDescriptor;
import eu.qualimaster.infrastructure.PipelineOptions;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by Apostolos Nydriotis on 12/11/14.
 */
public class TopoSoftwareCorrelationFinancial extends AbstractFinancialSubTopology
    implements ITopologyCreate, IScalableTopology {

  private String prefix;
  private String namespace;
  private String mapperName;
  private String hayashiYoshidaName;
  private IScalingDescriptor descriptor;

  public TopoSoftwareCorrelationFinancial() {

    prefix = "TopoSoftwareCorrelationFinancial";
    namespace = "PriorityPip";
    mapperName = prefix + "MapperBolt";
    hayashiYoshidaName = prefix + "HayashiYoshidaBolt";

    descriptor = new IScalingDescriptor() {

      private static final long serialVersionUID = -2776013131009037533L;

      @Override
      public Map<String, Integer> getScalingResult(double factor, boolean executors) {
        // keep mapper at 1 and scale hyBolts
        // here executors = tasks, you may adjust this if needed
        Map<String, Integer> result = new HashMap<String, Integer>();
        result.put(mapperName, 1); // always
        result.put(hayashiYoshidaName, (int) (1 * factor) - 1);
        return result;
      }

      @Override
      public Map<String, Integer> getScalingResult(int oldExecutors, int newExecutors,
                                                   boolean diffs) {
        // keep mapper at 1 and scale hyBolts
        // before we had oldExecutors, now we will have newExecutors
        Map<String, Integer> result = new HashMap<String, Integer>();
        result.put(mapperName, 1); // always
        if (diffs) {
          result.put(hayashiYoshidaName, newExecutors - oldExecutors - 1);
        } else {
          result.put(hayashiYoshidaName, newExecutors - 1);
        }
        return result;
      }

    };

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

    int parallelism_hint = PipelineOptions.getExecutorParallelism(config,
                                                                  prefix + "." + hayashiYoshidaName,
                                                                  4);
    int tasks = PipelineOptions.getTaskParallelism(config, prefix + "." + hayashiYoshidaName, 4);

    topologyBuilder.setBolt(hayashiYoshidaName,
                            new HayashiYoshidaBolt(hayashiYoshidaName, namespace, true, streamId),
                            parallelism_hint)
        .setNumTasks(tasks)
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

  @Override
  public IScalingDescriptor getScalingDescriptor() {
    return descriptor;
  }

}
