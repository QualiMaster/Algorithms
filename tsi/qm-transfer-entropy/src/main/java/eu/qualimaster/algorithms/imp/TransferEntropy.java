package eu.qualimaster.algorithms.imp;

import eu.qualimaster.families.inf.IFTransferEntropy;
import eu.qualimaster.observables.IObservable;
import gr.tuc.softnet.te.TEPair;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by justme on 29/9/2016.
 */
public class TransferEntropy implements IFTransferEntropy {
  private HashSet<String> ids = new HashSet<>();
  private HashMap<String, Double> lastValues = new HashMap<>();
  private HashMap<String, TEPair> allPairs = new HashMap<>();
  private int bins;
  private int multiplier; // Multiply the first value we get by this in order to get the max value
  private int numberOfBW;
  private int interval; // Interval (in sec) between TE calcs
  private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");

  public TransferEntropy() {
    interval = 10;
    numberOfBW = 30;
    init();
  }

  private void init() {
    bins = 32;
    multiplier = 2;
    ids = new HashSet<>();
    lastValues = new HashMap<>();
    allPairs = new HashMap<>();
  }

  @Override public void calculate(IIFTransferEntropyPreprocessedStreamInput input,
    IIFTransferEntropyPairwiseFinancialOutput pairwiseFinancialResult) {
    pairwiseFinancialResult.clear();
    String id = input.getSymbolId();
    double value = input.getValue();
    long timestamp = input.getTimestamp();
    lastValues.put(id, value); // Overwrite if exists
    boolean contains = ids.contains(id);
//    pairwiseFinancialResult.setValue(-1); // Just to know when the first output tuple is added
    boolean firstOutput = true;
    IIFTransferEntropyPairwiseFinancialOutput further;

    // TODO remove this block. Just for testing. Ignore new ids if we already have 300
    if(!contains && ids.size() >= 300) {
      pairwiseFinancialResult.noOutput();
      return;
    }

    for (String otherId : ids) {
      if (id.equals(otherId)) {
        continue;
      }
      TEPair pair;
      if (!contains) {
        pair =
          new TEPair(id, otherId, bins, value / multiplier, multiplier * value, lastValues.get(otherId) / multiplier,
            multiplier * lastValues.get(otherId), numberOfBW, interval);
        addToPairs(allPairs, id, otherId, pair);
      } else {
        pair = allPairs.get(getPairKey(id, otherId));
      }
      if (pair.processNewValue(id, value, timestamp)) {
        if (Math.abs(pair.getTEyx()) > 0) {
          if (firstOutput) {
            pairwiseFinancialResult.setId0(pair.getStreamY());
            pairwiseFinancialResult.setId1(pair.getStreamX());
            pairwiseFinancialResult.setDate(dateFormat.format(new Date()));
            pairwiseFinancialResult.setValue(pair.getTEyx());
            firstOutput = false;
          } else {
            further = pairwiseFinancialResult.addFurther();
            further.setId0(pair.getStreamY());
            further.setId1(pair.getStreamX());
            further.setDate(dateFormat.format(new Date()));
            further.setValue(pair.getTEyx());
          }
        }

        if (Math.abs(pair.getTExy()) > 0) {
          if (firstOutput) {
            pairwiseFinancialResult.setId0(pair.getStreamX());
            pairwiseFinancialResult.setId1(pair.getStreamY());
            pairwiseFinancialResult.setDate(dateFormat.format(new Date()));
            pairwiseFinancialResult.setValue(pair.getTExy());
            firstOutput = false;
          } else {
            further = pairwiseFinancialResult.addFurther();
            further.setId0(pair.getStreamX());
            further.setId1(pair.getStreamY());
            further.setDate(dateFormat.format(new Date()));
            further.setValue(pair.getTExy());
          }
        }
      }
    }
    if (!contains) {
      ids.add(id);
    }
    if (firstOutput) {
      pairwiseFinancialResult.noOutput();
    }
  }

  private static void addToPairs(HashMap<String, TEPair> allPairs, String id, String otherId, TEPair pair) {
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
    for(TEPair p : allPairs.values()) {
      p.setNumberOfBasicWindows(numberOfBW);
    }
  }

  @Override public void setParameterWindowAdvance(int value) {
    interval = value;
    for(TEPair p : allPairs.values()) {
      p.setUpdate_TE_every(interval);
    }
  }

  @Override public void setParameterDensitySize(int i) {
    init();
    bins = i;
  }

  @Override public void switchState(State state) {

  }

  @Override public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
