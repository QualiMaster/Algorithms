package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractSubTopology;
import eu.qualimaster.common.signal.BaseSignalBolt;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.ValueFormatException;
import eu.qualimaster.families.imp.FCorrelationTwitter;
import eu.qualimaster.families.inf.IFCorrelationFinancial;
import eu.qualimaster.families.inf.IFCorrelationTwitter;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Apostolos Nydriotis on 12/11/14.
 */

/*
 * Parallelism should always be 1!
 */
public class MapperBolt extends BaseSignalBolt {

  private OutputCollector collector;
  private Map<String, Set<Integer>> streamTaskMapping = new HashMap<>();
  private Map<Integer, List<Pair<String, String>>> taskStreamPairsMapping = new HashMap<>();
  private List<Integer> taskIds;
  private Logger logger = LoggerFactory.getLogger(MapperBolt.class);
  private String subTopologyPrefix;
  private boolean hasInitialized;
  private long windowStart;
  private long windowSize;
  private long windowAdvance;
  private long lastSeenTimestamp;
  private boolean terminating;

  public MapperBolt(String name, String namespace, String subTopologyPrefix) {
    super(name, namespace);
    this.subTopologyPrefix = subTopologyPrefix;
    terminating = false;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
    collector = outputCollector;
    taskIds = topologyContext.getComponentTasks(subTopologyPrefix + "HayashiYoshidaBolt");
    hasInitialized = false;
    System.out.println("taskIds = " + taskIds);

    windowStart = 0;
    windowSize = 30 * 1000;
    windowAdvance = 1 * 1000;
  }

  @Override
  public void execute(Tuple tuple) {
    if (terminating) return;
    if (tuple.getValue(0) instanceof IFCorrelationFinancial.IIFCorrelationFinancialSymbolListInput
        || tuple
            .getValue(0) instanceof IFCorrelationTwitter.IIFCorrelationTwitterSymbolListInput) {

      if (!hasInitialized) {

        hasInitialized = true;

        // configurationStream
        List<String> allSymbols;

        if (tuple.getValue(0)
            instanceof IFCorrelationFinancial.IIFCorrelationFinancialSymbolListInput) {
          allSymbols =
              ((IFCorrelationFinancial.IIFCorrelationFinancialSymbolListInput) tuple.getValue(0))
                  .getAllSymbols();
        } else {
          allSymbols =
              ((FCorrelationTwitter.IIFCorrelationTwitterSymbolListInput) tuple.getValue(0))
                  .getAllSymbols();
        }

        EmitMappingConfiguration(allSymbols, tuple);
      }
    } else if (tuple.getValue(0)
                   instanceof IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput
               || tuple.getValue(0)
                   instanceof IFCorrelationTwitter.IIFCorrelationTwitterAnalyzedStreamInput) {
      if (hasInitialized) {
        // symbolsStream

        String symbolId;
        long timestamp;
        double value;

        if (tuple.getValue(0)
            instanceof IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput) {

          IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput s =
              (IFCorrelationFinancial.IIFCorrelationFinancialPreprocessedStreamInput) tuple
                  .getValue(
                      0);

          symbolId = s.getSymbolId();
          timestamp = s.getTimestamp();
          value = s.getValue();
        } else {
          IFCorrelationTwitter.IIFCorrelationTwitterAnalyzedStreamInput s =
              (IFCorrelationTwitter.IIFCorrelationTwitterAnalyzedStreamInput) tuple.getValue(0);
          symbolId = s.getSymbolId();
          timestamp = s.getTimestamp();
          value = s.getValue();
        }

        if (timestamp < lastSeenTimestamp) {  // Prevent out-of-sync data (not exactly right...)
          timestamp = lastSeenTimestamp;
        } else {
          lastSeenTimestamp = timestamp;
        }

        if (windowStart == 0) {
          windowStart = timestamp;
        }

        while (windowStart + windowSize <= timestamp && !terminating) {
          collector.emit("resetWindowStream", tuple,  new Values(windowStart));
          windowStart += windowAdvance;
        }

        ForwardSymbol(symbolId, timestamp, value, tuple);
      } else {
        logger.error("Not initialized yet!");
      }
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // The symbols
    outputFieldsDeclarer.declareStream("symbolsStream", true, new Fields("streamId", "timestamp",
                                                                         "value"));

    // Configuration regarding which task will calculate which pairs (matrix partitioning)
    outputFieldsDeclarer.declareStream("configurationStream", true, new Fields("streamId0",
                                                                               "streamId1"));

    outputFieldsDeclarer.declareStream("resetWindowStream", false, new Fields("windowStart"));

//    outputFieldsDeclarer.declareStream("allSymbolsStringStream", false,
//                                       new Fields("allSymbolsString"));
  }

  /**
   * Maps each stream to the tasks that will use it for calculating a part of the correlation matrix
   * Partition the table to blocks by iterating each diagonal of the table
   */
  private void mapStreamsToTasks() {
    String[] streamsArray = new String[streamTaskMapping.size()];
    streamTaskMapping.keySet().toArray(streamsArray);
    int taskIndex = 0;

    int taskCount = taskIds.size();
    int streamsCount = streamsArray.length;
    int blockDimension = (int) Math.ceil((double) streamsCount / (double) taskCount);
    int diagonalsCount = (int) Math.ceil((double) streamsCount / (double) blockDimension);

    for (int d = 0; d < diagonalsCount; d++) {
      for (int iBlockCtr = 0; iBlockCtr < diagonalsCount - d; iBlockCtr++) {
        int jBlockCtr = iBlockCtr + d;
        for (int i = iBlockCtr * blockDimension;
             i < streamsCount && i < (iBlockCtr + 1) * blockDimension; i++) {

          for (int j = jBlockCtr == iBlockCtr ? i + 1 : jBlockCtr * blockDimension + 1;
               j < streamsCount && j < (jBlockCtr + 1) * blockDimension + 1;
               j++) {

            streamTaskMapping.get(streamsArray[i]).add(taskIds.get(taskIndex));
            streamTaskMapping.get(streamsArray[j]).add(taskIds.get(taskIndex));

            if (!taskStreamPairsMapping.containsKey(taskIds.get(taskIndex))) {
              taskStreamPairsMapping.put(taskIds.get(taskIndex),
                                         new ArrayList<Pair<String, String>>());
            }
            taskStreamPairsMapping.get(taskIds.get(taskIndex)).add(
                new Pair<String, String>(streamsArray[i], streamsArray[j]));
          }
        }
        if (++taskIndex == taskIds.size()) {
          taskIndex = 0;
        }
      }
    }
  }

  private void ForwardSymbol(String id, long timestamp, double value, Tuple tuple) {  // tuple for anchoring

    Set<Integer> targetTasks = streamTaskMapping.get(id);
    if (targetTasks == null || targetTasks.size() == 0) {
      logger.error("No target tasks!");
    } else {
      for (int target : targetTasks) {
        collector.emitDirect(target, "symbolsStream", tuple, new Values(id, timestamp, value));
      }
    }
  }

  private void EmitMappingConfiguration(List<String> symbols, Tuple tuple) {  // tuple for anchoring

//    String allSymbolsString = "";
    streamTaskMapping = new HashMap<>();
    for (String s : symbols) {
      streamTaskMapping.put(s, new HashSet<Integer>());
//      allSymbolsString += s + ",";
    }

//    if (allSymbolsString.length() > 0) {
//      allSymbolsString = allSymbolsString.substring(0, allSymbolsString.length() - 1) + "!";
//    }

//    collector.emit("allSymbolsStringStream", new Values(allSymbolsString));

    mapStreamsToTasks();

    for (Map.Entry<Integer, List<Pair<String, String>>> entry : taskStreamPairsMapping.entrySet()) {
      Integer target = entry.getKey();
      for (Pair<String, String> p : entry.getValue()) {
        String streamId0 = p.getKey();
        String streamId1 = p.getValue();
        collector.emitDirect(target, "configurationStream", tuple, new Values(streamId0, streamId1));
      }
      logger.info("Task " + target + "will calculate " + entry.getValue().size() + " pairs");
    }
  }

  @Override
  public void notifyParameterChange(ParameterChangeSignal signal) {
    logger.info("in notifyParameterChange");
    try {
      for (int i = 0; i < signal.getChangeCount(); i++) {
        ParameterChange parameterChange = signal.getChange(i);
        logger.info("Got parameterChange: " + parameterChange.getName());
        switch (parameterChange.getName()) {
          case "windowSize": {
            windowSize = parameterChange.getIntValue() * 1000;
            logger.info("Changed windowSize parameter to: " + windowSize);
            break;
          }
          case AbstractSubTopology.TERMINATION_PARAMETER: {
            // TODO(ap0n): Maybe there is something better that that
            terminating = true;
            break;
          }
          default: {
            continue;
          }
        }
      }
    } catch (ValueFormatException e) {
      e.printStackTrace();
    }
  }
}
