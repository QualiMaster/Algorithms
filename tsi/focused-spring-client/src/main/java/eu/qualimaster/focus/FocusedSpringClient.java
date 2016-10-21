package eu.qualimaster.focus;

import eu.qualimaster.algorithms.imp.correlation.SpringClient;
import eu.qualimaster.data.imp.FocusFincancialData;
import eu.qualimaster.data.imp.SpringFinancialData;
import eu.qualimaster.data.inf.IFocusFincancialData;
import eu.qualimaster.data.inf.ISpringFinancialData;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by ap0n on 16/2/2016.
 */
public class FocusedSpringClient implements IFocusFincancialData {

  private SpringClient springClient;
  private Logger logger = LoggerFactory.getLogger(FocusedSpringClient.class);

  public FocusedSpringClient() {
    this.springClient = new SpringClient();
  }

  @Override
  public IFocusFincancialDataSymbolListOutput getSymbolList() {

    ISpringFinancialData.ISpringFinancialDataSymbolListOutput a = springClient.getSymbolList();

    if (a != null) {
      FocusFincancialData.FocusFincancialDataSymbolListOutput output =
          new FocusFincancialData.FocusFincancialDataSymbolListOutput();
      output.setAllSymbols(a.getAllSymbols());
      return output;
    }
    return null;
  }

  @Override
  public String getAggregationKey(IFocusFincancialDataSymbolListOutput tuple) {
    return null;
  }

  @Override
  public IFocusFincancialDataSpringStreamOutput getSpringStream() {

    ISpringFinancialData.ISpringFinancialDataSpringStreamOutput a = springClient.getSpringStream();

    if (a != null) {
      FocusFincancialData.FocusFincancialDataSpringStreamOutput output =
          new FocusFincancialData.FocusFincancialDataSpringStreamOutput();
      output.setSymbolTuple(springClient.getSpringStream().getSymbolTuple());
      return output;
    }
    return null;
  }

  @Override
  public String getAggregationKey(IFocusFincancialDataSpringStreamOutput tuple) {

    ISpringFinancialData.ISpringFinancialDataSpringStreamOutput
        a = new SpringFinancialData.SpringFinancialDataSpringStreamOutput();
    a.setSymbolTuple(tuple.getSymbolTuple());
    return springClient.getAggregationKey(a);
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
          springClient.startQuote(player);
        } else {
          springClient.stopQuote(player);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void connect() {
    springClient.init("focusedSpringClient");
  }

  @Override
  public void disconnect() {
    springClient.disconnect();
  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return springClient.getStrategy();
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {
    springClient.setStrategy(iStorageStrategyDescriptor);
  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return springClient.getMeasurement(iObservable);
  }

  @Override
  public IHistoricalDataProvider getHistoricalDataProvider() {
    return null;
  }

  @Override
  public Map<String, String> getIdsNamesMap() {
    return springClient.getIdsNamesMap();
  }

  @Override
  public void setDataSourceListener(IDataSourceListener iDataSourceListener) {
    springClient.setDataSourceListener(iDataSourceListener);
  }
}
