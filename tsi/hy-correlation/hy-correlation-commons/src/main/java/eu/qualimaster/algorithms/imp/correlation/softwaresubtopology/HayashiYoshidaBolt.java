package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology;

import eu.qualimaster.algorithms.imp.correlation.AbstractSubTopology;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.Interval;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.NominatorMatrix;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.OverlapsMatrix;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.Stream;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.StreamTuple;
import eu.qualimaster.common.signal.BaseSignalBolt;
import eu.qualimaster.common.signal.ParameterChange;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.families.imp.FCorrelationFinancial;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Apostolos Nydriotis on 12/11/14.
 */
public class HayashiYoshidaBolt extends BaseSignalBolt {

  private final String streamId;
  private transient OutputCollector collector;
  private String typeFlag;  // "w" or "f" for web or financial data

  private Logger logger = LoggerFactory.getLogger(HayashiYoshidaBolt.class);
  private int taskId;
  private OverlapsMatrix overlapsMatrix;  // The matrix that stores all the interval overlaps
  private Map<String, Stream> streams;  // The data kept for each stream
  private Set<Pair<String, String>> streamPairs;  // The stream pairs that will be processed by this
  //  task
  private NominatorMatrix nominators;  // The matrix that stores the contribution to the nominator
  // of the estimator for of each overlapped interval
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
//  private PrintWriter logPrinter;
  private boolean terminating;

  public HayashiYoshidaBolt(String name, String namespace, Boolean isFinancial, String streamId) {
    super(name, namespace);

    this.streamId = streamId;
    if (isFinancial) {
      this.typeFlag = "f";
    } else {
      this.typeFlag = "w";
    }
  }

  @Override
  public void prepare(Map config, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    super.prepare(config, topologyContext, outputCollector);
    collector = outputCollector;
    taskId = topologyContext.getThisTaskId();

    terminating = false;
    if (null == nominators) {
        nominators = new NominatorMatrix();
    }
    if (null == overlapsMatrix) {
        overlapsMatrix = new OverlapsMatrix();
    }
    if (null == streams) {
        streams = new HashMap<>();
    }
    if (null == streamPairs) {
        streamPairs = new HashSet<>();
    }
    
//    try {
//      logPrinter =
//          new PrintWriter(new FileOutputStream(
//              new File("/var/nfs/ttt/" + InetAddress.getLocalHost().getHostName() + "-" + Thread
//                  .currentThread().getId() + ".log"), true));
//    } catch (FileNotFoundException e) {
//      e.printStackTrace();
//    } catch (UnknownHostException e) {
//      e.printStackTrace();
//    }
//    logPrinter.println("Started taskId: " + taskId);
//    logPrinter.println("Tasks: " + topologyContext.getComponentTasks(topologyContext.getThisComponentId()));
  }

  @Override
  public void execute(Tuple tuple) {
    if (terminating) return;
    String sourceStreamId = tuple.getSourceStreamId();
//    logPrinter.println(taskId + " got from " + tuple.getSourceStreamId());
    if (sourceStreamId.equals("symbolsStream")) {  // Symbol tuple

      String streamId = tuple.getStringByField("streamId");
      long timestamp = tuple.getLongByField("timestamp");
      double value = tuple.getDoubleByField("value");

      updateNominator(streamId, new StreamTuple(value, timestamp));

    } else if (sourceStreamId.equals("resetWindowStream")) {  // Reset window tuple
      //      Date before = new Date();

      Stream[] streamArray = new Stream[streams.size()];
      streams.values().toArray(streamArray);

      long windowStart = tuple.getLongByField("windowStart");

      shiftWindow(windowStart, streamArray);

      calculateCorrelations(streamArray, tuple);

    } else if (sourceStreamId.equals("configurationStream")) {  // Config tuple
      streamPairs.add(new Pair<>(tuple.getStringByField("streamId0"),
                                 tuple.getStringByField("streamId1")));
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // Outputs a string as "symbol0,symbol1,correlation"
    outputFieldsDeclarer.declareStream(streamId, false, new Fields("correlation"));
  }

  private void updateNominator(String streamId, StreamTuple newTuple) {

    Stream currentStream = streams.get(streamId);

    if (currentStream == null) {
      currentStream = new Stream(streamId);
      streams.put(streamId, currentStream);
    }

    if (!currentStream.addTuple(newTuple)) {  // This tuple has been received again (ignore the new
      // values)
      return;
    }

    if (currentStream.getIntervalsCount() == 0) {  // First tuple for this stream
      return;
    }

    Interval newInterval = currentStream.getInterval(currentStream.getIntervalsCount() - 1);

    for (Map.Entry<String, Stream> pair : streams.entrySet()) {

      if (terminating) return;

      String otherStreamId = pair.getKey();

      if (otherStreamId.equals(streamId)) {
        continue;  // Do not calculate Corr(sX, sX)
      }

      if (!streamPairs.contains(new Pair<>(streamId, otherStreamId))
          && !streamPairs.contains(new Pair<>(otherStreamId, streamId))) {
        continue;  // this pair will be calculated by another task
      }

      List<Interval> otherStreamIntervals = pair.getValue().getIntervals();

      for (int i = otherStreamIntervals.size() - 1; i >= 0; i--) {
        Interval otherInterval = otherStreamIntervals.get(i);
        if (intervalsOverlap(newInterval, otherInterval)) {

          // If intervals overlap, calculate their contribution to the estimator's nominator
          double nominator = nominators.getNominator(streamId, otherStreamId);
          nominator += newInterval.getDelta() * otherInterval.getDelta();
          nominators.addNominator(streamId, otherStreamId, nominator);

          // Store that the intervals overlap (useful when the window will expire)
          overlapsMatrix.addOverlap(streamId, otherStreamId, newInterval.getIndex(),
                                    otherInterval.getIndex(), true);
        }
      }
    }
  }

  private boolean intervalsOverlap(Interval a, Interval b) {
    if (a.getBeginning() >= b.getEnd() || a.getEnd() <= b.getBeginning()) {
      return false;
    }
    return true;
  }

  private void calculateCorrelations(Stream[] streams, Tuple tuple) {  // tuple for anchoring
    double correlation;
    for (int i = 0; i < streams.length; i++) {
      Stream s0 = streams[i];
      for (int j = i + 1; j < streams.length; j++) {
        if (terminating) return;

        Stream s1 = streams[j];

        if (!streamPairs.contains(new Pair<>(s0.getId(), s1.getId()))
            && !streamPairs.contains(new Pair<>(s1.getId(), s0.getId()))) {
          continue;  // this pair will be calculated by another task
        }

        if (s0.getDeltaSquaredSum() > 0 && s1.getDeltaSquaredSum() > 0) {
          double denominator =
              Math.sqrt(s0.getDeltaSquaredSum()) * Math.sqrt(s1.getDeltaSquaredSum());
          correlation = nominators.getNominator(s0.getId(), s1.getId()) / denominator;
          correlation = Math.max(Math.min(correlation, 1.0), -1.0);
        } else {
          continue;
        }
        FCorrelationFinancial.IFCorrelationFinancialPairwiseFinancialOutput
            ifCorrelationFinancialOutput =
            new FCorrelationFinancial.IFCorrelationFinancialPairwiseFinancialOutput();

        // If necessary flip the ids according to lexicographic order e.g. (b,a)->(a,b)
        String firstId = s0.getId().compareTo(s1.getId()) < 0 ? s0.getId() : s1.getId();
        String secondId = s0.getId().compareTo(s1.getId()) < 0 ? s1.getId() : s0.getId();

//        String result = firstId + "," + secondId
//                        + "," + dateFormat.format(new Date())
//                        + "," + new DecimalFormat("0.000000").format(correlation);

        ifCorrelationFinancialOutput.setId0(firstId);
        ifCorrelationFinancialOutput.setId1(secondId);
        ifCorrelationFinancialOutput.setDate(dateFormat.format(new Date()));
        ifCorrelationFinancialOutput.setValue(correlation);
//        monitorMe();
//        logger.info("correlation " + result);
        collector.emit(streamId, /*tuple,*/ new Values(ifCorrelationFinancialOutput));
      }
    }
  }

  private void shiftWindow(long windowStart, Stream[] streams) {
    for (int i = 0; i < streams.length; i++) {

      if (terminating) return;

      Stream s0 = streams[i];
      for (Iterator<Interval> outerIt = s0.getIntervals().iterator(); outerIt.hasNext(); ) {
        Interval outerInterval = outerIt.next();
        if (outerInterval.getEnd() < windowStart) {
          // interval's contribution to the estimator must be undone
          for (int j = 0; j < streams.length; j++) {
            if (j == i) {
              continue;
            }
            Stream s1 = streams[j];
            for (Iterator<Interval> innerIt = s1.getIntervals().iterator(); innerIt.hasNext(); ) {
              Interval innerInterval = innerIt.next();
              if (overlapsMatrix.doOverlap(s0.getId(), s1.getId(), outerInterval.getIndex(),
                                           innerInterval.getIndex())) {
                double nominator = nominators.getNominator(s0.getId(), s1.getId());
                nominator -= outerInterval.getDelta() * innerInterval.getDelta();
                nominators.addNominator(s0.getId(), s1.getId(), nominator);
                overlapsMatrix.removeOverlap(s0.getId(), s1.getId(), outerInterval.getIndex(),
                                             innerInterval.getIndex());
              } else {
                break;
              }
            }
          }
          s0.removeDeltaContribution(outerInterval.getDelta());
          s0.removeInterval(outerIt);
        } else {
          break;
        }
      }
    }
  }

  @Override
  public void notifyParameterChange(ParameterChangeSignal signal) {
    logger.info("in notifyParameterChange");
    for (int i = 0; i < signal.getChangeCount(); i++) {
      ParameterChange parameterChange = signal.getChange(i);
      logger.info("Got parameterChange: " + parameterChange.getName());
      switch (parameterChange.getName()) {
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
  }

//  private void monitorMe() {
//    if (financialMonitoringTimestamp == 0) {
//      financialMonitoringTimestamp = new Date().getTime();
//      ++financialThroughput;
//    } else {
//      long now = new Date().getTime();
//      if (now - financialMonitoringTimestamp < measurementDuration * 1000) {
//        ++financialThroughput;
//      } else {
//        logger.info("Pipeline financial output throughput: "
//                    + ((double) financialThroughput / (double)measurementDuration) + " tuples/sec");
//        financialMonitoringTimestamp = now;
//        financialThroughput = 1;
//      }
//    }
//  }
}
