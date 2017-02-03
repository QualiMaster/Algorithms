package eu.qualimaster.algorithms.imp;

import eu.qualimaster.families.inf.IFTransferEntropy;
import eu.qualimaster.observables.IObservable;
import gr.tuc.softnet.te.streaming.TEPairStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by justme on 29/9/2016.
 */
public class TransferEntropy implements IFTransferEntropy {

  private Logger logger = LoggerFactory.getLogger(TransferEntropy.class);
  private HashSet<String> ids;
  private HashMap<String, Double> lastValues;
  private HashMap<String, TEPairStreaming> allPairs;
  private HashMap<String, Double> lastEmitted;
  private int bins;
  private double multiplier; // Multiply the first value we get by this in order to get the max value
  private int numberOfBW;
  private int interval; // Interval (in sec) between TE calcs
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
  private static final double RESULT_CHANGED_PERCENTAGE = 0.01;

  public TransferEntropy() {
    interval = 10;
    numberOfBW = 30;
    init();
  }

  private void init() {
    bins = 30;
    multiplier = 1.2;
    ids = new HashSet<>();
    lastValues = new HashMap<>();
    allPairs = new HashMap<>();
    lastEmitted = new HashMap<>();
  }

  @Override public void calculate(IIFTransferEntropyPreprocessedStreamInput input,
    IIFTransferEntropyPairwiseFinancialOutput pairwiseFinancialResult) {
    pairwiseFinancialResult.clear();
    String id = input.getSymbolId();
    double value = input.getValue();
    long timestamp = input.getTimestamp();
    lastValues.put(id, value); // Overwrite if exists
    boolean contains = ids.contains(id);
    boolean firstOutput = true;

    IIFTransferEntropyPairwiseFinancialOutput further;

    // TODO remove this block. Just for testing. Ignore new ids if we already have X
    if (!contains && ids.size() >= 500) {
      pairwiseFinancialResult.noOutput();
      return;
    }

    for (String otherId : ids) {
      if (id.equals(otherId)) {
        continue;
      }
      TEPairStreaming pair;
      if (!contains) {
        pair = new TEPairStreaming(id, otherId, bins, value / multiplier, multiplier * value,
          lastValues.get(otherId) / multiplier, multiplier * lastValues.get(otherId));
        addToPairs(allPairs, id, otherId, pair);
      } else {
        pair = allPairs.get(getPairKey(id, otherId));
      }
      pair.processNewValue(id, value, timestamp);

      if (pair.isWarmedUp()) {
        firstOutput = appendToOutputAndUpdateFlag(pairwiseFinancialResult, firstOutput, pair);
      }
    }
    if (!contains) {
      ids.add(id);
    }
    if (firstOutput) {
      pairwiseFinancialResult.noOutput();
    }
  }

  private boolean appendToOutputAndUpdateFlag(IIFTransferEntropyPairwiseFinancialOutput pairwiseFinancialResult,
    boolean firstOutput, TEPairStreaming pair) {

    firstOutput =
      appendToOutput(pairwiseFinancialResult, firstOutput, pair.getStreamY(), pair.getStreamX(), pair.getTEyx());

    firstOutput =
      appendToOutput(pairwiseFinancialResult, firstOutput, pair.getStreamX(), pair.getStreamY(), pair.getTExy());

    return firstOutput;
  }

  private boolean appendToOutput(IIFTransferEntropyPairwiseFinancialOutput pairwiseFinancialResult, boolean firstOutput,
    String first, String second, double te) {
    IIFTransferEntropyPairwiseFinancialOutput further;
    String pairDirectedKey = first + "," + second;
    double lastEmittedValue = lastEmitted.containsKey(pairDirectedKey) ? lastEmitted.get(pairDirectedKey) : 0.0;

    double abs = Math.abs(te);
    if (lastEmittedValue == 0.0 || (abs > 0
      && Math.abs(te - lastEmittedValue) / lastEmittedValue > RESULT_CHANGED_PERCENTAGE)) {
      lastEmitted.put(pairDirectedKey, te);
      if (firstOutput) {
        pairwiseFinancialResult.setId0(first);
        pairwiseFinancialResult.setId1(second);
        pairwiseFinancialResult.setDate(dateFormat.format(new Date()));
        pairwiseFinancialResult.setValue(te);
        firstOutput = false;
      } else {
        further = pairwiseFinancialResult.addFurther();
        further.setId0(first);
        further.setId1(second);
        further.setDate(dateFormat.format(new Date()));
        further.setValue(te);
      }
    }
    return firstOutput;
  }

  private void addToPairs(HashMap<String, TEPairStreaming> allPairs, String id, String otherId, TEPairStreaming pair) {
    String pairKey = getPairKey(id, otherId);
    allPairs.put(pairKey, pair);
  }

  private static String getPairKey(String id, String otherId) {
    String pairKey = id + "," + otherId;
    if (id.compareTo(otherId) < 0) {
      pairKey = otherId + "," + id;
    }
    return pairKey;
  }

  @Override public void calculate(IIFTransferEntropySymbolListInput input,
    IIFTransferEntropyPairwiseFinancialOutput pairwiseFinancialResult) {
    //Do nothing
    pairwiseFinancialResult.noOutput();
  }

  @Override public void setParameterWindowSize(int i) {
    numberOfBW = i / interval;
    //    for (TEPairStreaming p : allPairs.values()) {
    //      p.setNumberOfBasicWindows(numberOfBW);
    //    }
  }

  @Override public void setParameterWindowAdvance(int value) {
    interval = value;
    //    for (TEPairStreaming p : allPairs.values()) {
    //      p.setUpdate_TE_every(interval);
    //    }
  }

  @Override public void setParameterDensitySize(int i) {
    // TODO uncomment us. Disabled for now
    //    init();
    //    bins = i;
  }

  @Override public void switchState(State state) {

  }

  @Override public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
