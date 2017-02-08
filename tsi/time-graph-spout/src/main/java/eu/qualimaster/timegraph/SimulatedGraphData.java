package eu.qualimaster.timegraph;

import eu.qualimaster.data.inf.ISimulatedGraphData;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;

import java.util.Map;

/**
 * Created by ap0n on 8/2/2017.
 */
public class SimulatedGraphData implements ISimulatedGraphData {

  @Override
  public ISimulatedGraphDataEdgeStreamOutput getEdgeStream() {
    return null;
  }

  @Override
  public String getAggregationKey(ISimulatedGraphDataEdgeStreamOutput tuple) {
    return null;
  }

  @Override
  public IHistoricalDataProvider getHistoricalDataProvider() {
    return null;
  }

  @Override
  public Map<String, String> getIdsNamesMap() {
    return null;
  }

  @Override
  public void setDataSourceListener(IDataSourceListener iDataSourceListener) {

  }

  @Override
  public void connect() {

  }

  @Override
  public void disconnect() {

  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
