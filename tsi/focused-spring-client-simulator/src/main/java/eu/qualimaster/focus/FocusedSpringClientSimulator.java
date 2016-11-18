package eu.qualimaster.focus;

import eu.qualimaster.algorithms.imp.correlation.SpringClientSimulator;
import eu.qualimaster.data.imp.SimulatedFinancialData;
import eu.qualimaster.data.imp.SimulatedFocusFinancialData;
import eu.qualimaster.data.inf.ISimulatedFinancialData;
import eu.qualimaster.data.inf.ISimulatedFocusFinancialData;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ap0n on 16/2/2016.
 */
public class FocusedSpringClientSimulator implements ISimulatedFocusFinancialData {

  private SpringClientSimulator springClientSimulator;
  private Set<String> filter;
  private Logger logger = LoggerFactory.getLogger(FocusedSpringClientSimulator.class);
  private long tupleCounter;

  public FocusedSpringClientSimulator() {
    this.springClientSimulator = new SpringClientSimulator();
    filter = new HashSet<>();
    tupleCounter = 0;
  }

  @Override
  public ISimulatedFocusFinancialDataSymbolListOutput getSymbolList() {

    ISimulatedFinancialData.ISimulatedFinancialDataSymbolListOutput
        a = springClientSimulator.getSymbolList();

    if (a != null) {
      ISimulatedFocusFinancialDataSymbolListOutput output =
          new SimulatedFocusFinancialData.SimulatedFocusFinancialDataSymbolListOutput();
      output.setAllSymbols(a.getAllSymbols());
      return output;
    }
    return null;
  }

  @Override
  public String getAggregationKey(ISimulatedFocusFinancialDataSymbolListOutput tuple) {
    return null;
  }

  @Override
  public ISimulatedFocusFinancialDataSpringStreamOutput getSpringStream() {

    ISimulatedFinancialData.ISimulatedFinancialDataSpringStreamOutput
        a = springClientSimulator.getSpringStream();
    if (a != null) {
      String[] fields = a.getSymbolTuple().split(",");
      String id = fields[0];
      if (filter(id)) {
        ISimulatedFocusFinancialDataSpringStreamOutput output =
            new SimulatedFocusFinancialData.SimulatedFocusFinancialDataSpringStreamOutput();
        output.setSymbolTuple(a.getSymbolTuple());
        if (tupleCounter++ % 100 == 0) {
          logger.info("Tuple date: " + fields[1] + "," + fields[2] + "\tReal date: " + new Date());
        }
        return output;
      }
    }
    return null;
  }

  @Override
  public String getAggregationKey(ISimulatedFocusFinancialDataSpringStreamOutput tuple) {
    ISimulatedFinancialData.ISimulatedFinancialDataSpringStreamOutput
        a = new SimulatedFinancialData.SimulatedFinancialDataSpringStreamOutput();
    a.setSymbolTuple(tuple.getSymbolTuple());
    return springClientSimulator.getAggregationKey(a);
  }

  @Override
  public void setParameterSpeedFactor(double v) {
    springClientSimulator.setSpeed(v);
  }

  @Override
  public void setParameterPlayerList(String value) {
    logger.info("Got playerList parameter. New value: " + value);
    String[] cmd = value.split("/");
    if (cmd.length != 2) {
      return;  // default value
    }
    for (String player : cmd[1].split(",")) {
      try {
        if (cmd[0].startsWith("add")) {
          filter.add(player);
        } else {
          filter.remove(player);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void connect() {
    springClientSimulator.connect();
  }

  @Override
  public void disconnect() {
    springClientSimulator.disconnect();
  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return springClientSimulator.getStrategy();
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {
    springClientSimulator.setStrategy(iStorageStrategyDescriptor);
  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return springClientSimulator.getMeasurement(iObservable);
  }

  private boolean filter(String key) {
    return filter.contains(key);
  }

  @Override
  public IHistoricalDataProvider getHistoricalDataProvider() {
    return null;
  }

  @Override
  public Map<String, String> getIdsNamesMap() {
    return springClientSimulator.getIdsNamesMap();
  }

  @Override
  public void setDataSourceListener(IDataSourceListener iDataSourceListener) {
    springClientSimulator.setDataSourceListener(iDataSourceListener);
  }
}
