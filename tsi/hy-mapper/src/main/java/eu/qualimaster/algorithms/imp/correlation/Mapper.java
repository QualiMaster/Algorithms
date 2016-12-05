package eu.qualimaster.algorithms.imp.correlation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.qualimaster.base.algorithm.IFamily;
import eu.qualimaster.families.inf.IFMapper;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.families.imp.*;

//SUH
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Calendar;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

/**
 * Mapper algorithm implementation for the software correlation financial subtopology.
 *
 */
public class Mapper implements IFMapper
{	  
    private Map<String, Set<Integer>> streamTaskMapping = new HashMap<String, Set<Integer>>();
    private Map<Integer, List<Pair<String, String>>> taskStreamPairsMapping = new HashMap<Integer, List<Pair<String, String>>>();
    private List<Integer> taskIds;
    private Logger logger = LoggerFactory.getLogger(Mapper.class);
    String symbolId;
    long timestamp;
    double value;
    private boolean hasInitialized = false;
    private long windowStart = 0;
    private long windowSize = 0;
    private long windowAdvance = 0;
    private long lastSeenTimestamp = 0;
    private List<String> allSymbols;
    
    public Mapper(List<Integer> taskIds, int thisTaskId) {
    	this(taskIds);  // Call the other constructor
    }

    public Mapper(List<Integer> taskIds) {
        hasInitialized = false;
        windowStart = 0;
        windowSize = 30 * 1000;
        windowAdvance = 1 * 1000;
        this.taskIds = taskIds;
    }
    
    public List<Integer> getTaskIds() {
        return taskIds;
    }

    public void setTaskIds(List<Integer> taskIds) {
        this.taskIds = taskIds;
    }

    public void switchState(State arg0) {
        // TODO Auto-generated method stub
        
    }

    public Double getMeasurement(IObservable observable) {
        // TODO Auto-generated method stub
        return null;
    }
    /**
     * Handle the symbol stream input.
     */
    public void calculate(IIFMapperPreprocessedStreamInput input,
            IIFMapperSymbolsStreamOutput symbolsStreamResult,
            IIFMapperConfigurationStreamOutput configurationStreamResult,
            IIFMapperResetWindowStreamOutput resetWindowStreamResult) {
        //clear the previous cached results
        symbolsStreamResult.clear();
        configurationStreamResult.clear();
        resetWindowStreamResult.clear();
        
        boolean first = true;
        IIFMapperResetWindowStreamOutput further;
        if(hasInitialized) {            
            symbolId = input.getSymbolId();
            timestamp = input.getTimestamp();
            value = input.getValue();

            if (timestamp < lastSeenTimestamp) {  // Prevent out-of-sync data (not exactly right...)
                timestamp = lastSeenTimestamp;
            } else {
                lastSeenTimestamp = timestamp;
            }

            if (windowStart == 0) {
                windowStart = timestamp;
            }
            
            if(windowStart + windowSize > timestamp) {
                resetWindowStreamResult.noOutput(); //no reset window stream result
            } else {
            	resetWindowStreamResult.noOutput(); //no reset window stream result
                while (windowStart + windowSize <= timestamp) {//return resetWindowStreamResult
                	//create new instance of result
                	IIFMapperResetWindowStreamOutput result = resetWindowStreamResult.createItem();
                	result.setWindowStart(windowStart);
                	
                	resetWindowStreamResult.emitDirect("MapperResetWindowStream", result);               	
                	/*
                	if(first) {
                        first = false;
                        resetWindowStreamResult.setWindowStart(windowStart);
                    } else {
                        further = resetWindowStreamResult.addFurther();
                        further.setWindowStart(windowStart);
                    }
                    */
                    windowStart += windowAdvance;
                }
            }
            
            ForwardSymbol(symbolId, timestamp, value, symbolsStreamResult);//return symbolsStreamResult, otherwise return no output
            
            configurationStreamResult.noOutput(); //no configuration stream related result
        } else {
            logger.error("Not initialized yet!");
            symbolsStreamResult.noOutput();
            configurationStreamResult.noOutput();
            resetWindowStreamResult.noOutput();
        }
    }
    
    /**
     * Handle the symbol list input.
     */
    public void calculate(IIFMapperSymbolListInput input,
            IIFMapperSymbolsStreamOutput symbolsStreamResult,
            IIFMapperConfigurationStreamOutput configurationStreamResult,
            IIFMapperResetWindowStreamOutput resetWindowStreamResult) {
        //clear the previous cached results
        symbolsStreamResult.clear();
        configurationStreamResult.clear();
        resetWindowStreamResult.clear();
        
        allSymbols = input.getAllSymbols();
        
        if(!hasInitialized) {
            hasInitialized = true;
            EmitMappingConfiguration(allSymbols, configurationStreamResult); //return configurationStreamResult
        } else {
            configurationStreamResult.noOutput(); //do not return any output
        }
        
        //if no related result, it shall return no output
        symbolsStreamResult.noOutput();
        resetWindowStreamResult.noOutput();
    }

    private void EmitMappingConfiguration(List<String> symbols, IIFMapperConfigurationStreamOutput configurationStreamResult) {  
        IIFMapperConfigurationStreamOutput further; //used for further output
        boolean first = true; 
        streamTaskMapping = new HashMap<String, Set<Integer>>();
        for (String s : symbols) {
          streamTaskMapping.put(s, new HashSet<Integer>());
        }

        mapStreamsToTasks();
        //return no output
        configurationStreamResult.noOutput();
        for (Map.Entry<Integer, List<Pair<String, String>>> entry : taskStreamPairsMapping.entrySet()) {
            Integer target = entry.getKey();
            for (Pair<String, String> p : entry.getValue()) {
                String streamId0 = p.getKey();
                String streamId1 = p.getValue();
                
                //SUH                
                //directly emit
                //create new instance of result
                IIFMapperConfigurationStreamOutput result = configurationStreamResult.createItem();
                
                result.setTaskId(target);//taskId for emitDirect
                result.setPairKey(streamId0);
                result.setPairValue(streamId1);
                
                configurationStreamResult.emitDirect("MapperConfigurationStream", result);
                //collector.emitDirect(target, "MapperConfigurationStream", new Values(result));
                
                //collector.emitDirect(target, "MapperConfigurationStream", new Values(streamId0, streamId1));
                
                /*
                if (first) {//set the first pair
                    first = false;
                    configurationStreamResult.setTaskId(target);//taskId for emitDirect
                    configurationStreamResult.setPairKey(streamId0);
                    configurationStreamResult.setPairValue(streamId1);
                } else {//set further pairs
                    further = configurationStreamResult.addFurther();// add multiple outputs
                    further.setTaskId(target);//taskId for emitDirect
                    further.setPairKey(streamId0);
                    further.setPairValue(streamId1);
                }
                */
                
            }
            logger.info("Task " + target + "will calculate "
                    + entry.getValue().size() + " pairs");
        }
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
    
    private void ForwardSymbol(String id, long timestamp, double value, IIFMapperSymbolsStreamOutput symbolsStreamResult) {  
        IIFMapperSymbolsStreamOutput further;
        boolean first = true;
        Set<Integer> targetTasks = streamTaskMapping.get(id);
        if (targetTasks == null || targetTasks.size() == 0) {
          logger.error("No target tasks!");
          symbolsStreamResult.noOutput();//therefore no symbol stream result
        } else {
          //return no output
          symbolsStreamResult.noOutput();
          for (int target : targetTasks) {
        	  //SUH
        	  //directly emit        	  
        	  //create new instance of result
        	  IIFMapperSymbolsStreamOutput result = symbolsStreamResult.createItem();
        	  
        	  result.setTaskId(target);
              result.setSymbolId(id);
              result.setTimestamp(timestamp);
              result.setValue(value);
        	  
        	  symbolsStreamResult.emitDirect("MapperSymbolsStream", result);
              //collector.emitDirect(target, "MapperSymbolsStream", new Values(result));
            
        	  //collector.emitDirect(target, "MapperSymbolsStream", new Values(id, timestamp, value));
              
        	  /*
              if(first) {
                  first = false;
                  symbolsStreamResult.setTaskId(target);
                  symbolsStreamResult.setSymbolId(id);
                  symbolsStreamResult.setTimestamp(timestamp);
                  symbolsStreamResult.setValue(value);
              } else {
                  further = symbolsStreamResult.addFurther();
                  further.setTaskId(target);
                  further.setSymbolId(id);
                  further.setTimestamp(timestamp);
                  further.setValue(value);
             }
             */
             
           }
        }
    }
    
    public void setParameterWindowSize(int value) {
        this.windowSize = value;
    }
    
}
