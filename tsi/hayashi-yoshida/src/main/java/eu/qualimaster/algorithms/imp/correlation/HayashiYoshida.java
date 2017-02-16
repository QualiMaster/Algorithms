package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.families.inf.IFHayashiYoshida;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.Interval;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.NominatorMatrix;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.OverlapsMatrix;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.Stream;
import eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons.StreamTuple;


import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

//use to log the output
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Calendar;
/**
 * HayashiYoshida algorithm implementation. 
 *
 */
public class HayashiYoshida implements IFHayashiYoshida 
{
    private Logger logger = LoggerFactory.getLogger(HayashiYoshida.class);
    private OverlapsMatrix overlapsMatrix;  // The matrix that stores all the interval overlaps
    private Map<String, Stream> streams;  // The data kept for each stream
    private Set<Pair<String, String>> streamPairs;  // The stream pairs that will be processed by this
                                                    //  task
    private NominatorMatrix nominators;  // The matrix that stores the contribution to the nominator
                                         // of the estimator for of each overlapped interval
    private DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
    Stream[] streamArray;
    long windowStart;
    String streamId;
    long timestamp;
    double value; 
    private boolean terminating;   
    
    public HayashiYoshida() {   
    	terminating = false;
        nominators = new NominatorMatrix();
        overlapsMatrix = new OverlapsMatrix();
        streams = new HashMap<String, Stream>();
        streamPairs = new HashSet<Pair<String, String>>();
    }    
    
    public void switchState(State state) {
        if(state == State.TERMINATING) {
        	terminating = true;
        }
    }

    public Double getMeasurement(IObservable observable) {
        // TODO Auto-generated method stub
        return null;
    }

    public void calculate(IIFHayashiYoshidaSymbolsStreamInput input,
            IIFHayashiYoshidaPairwiseFinancialOutput pairwiseFinancialResult) {
        //clear the previous cached result
        pairwiseFinancialResult.clear();
        
        if (terminating) return;
        
        streamId = input.getSymbolId();
        timestamp = input.getTimestamp();
        value = input.getValue();        
        
        updateNominator(streamId, new StreamTuple(value, timestamp));
        
        //no pairwiseFinancialResult being returned
        pairwiseFinancialResult.noOutput();
    }

    public void calculate(IIFHayashiYoshidaConfigurationStreamInput input,
            IIFHayashiYoshidaPairwiseFinancialOutput pairwiseFinancialResult) {
        //clear the previous cached result
        pairwiseFinancialResult.clear();        
    	
        if (terminating) return;
        
        String key = input.getPairKey();
        String value = input.getPairValue();
        if(key != null && value != null) {
        	if(key.length() == 0 && value.length() == 0 ) {
        		streamPairs.add(new Pair<String, String>(input.getPairKey(),
                input.getPairValue()));
        	}
        }
        //no pairwiseFinancialResult being returned
        pairwiseFinancialResult.noOutput();
    }

    public void calculate(IIFHayashiYoshidaResetWindowStreamInput input,
            IIFHayashiYoshidaPairwiseFinancialOutput pairwiseFinancialResult) {
        logger.info("GOT RESET WINDOW! (HY)");
        
        //clear the previous cached result
        pairwiseFinancialResult.clear();
        
        if (terminating) return;
        
        streamArray = new Stream[streams.size()];
        streams.values().toArray(streamArray);
        windowStart = input.getWindowStart();      
        
        shiftWindow(windowStart, streamArray);
        
        calculateCorrelations(streamArray, pairwiseFinancialResult); //return pairwiseFinancialResult
    }
    
    private void updateNominator(String streamId, StreamTuple newTuple) {
        Stream currentStream = streams.get(streamId);

        if (currentStream == null) {
            currentStream = new Stream(streamId);
            streams.put(streamId, currentStream);
        }

        if (!currentStream.addTuple(newTuple)) {  // This tuple has been received again (ignore the new values)
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

            if (!streamPairs.contains(new Pair<String, String>(streamId, otherStreamId))
                && !streamPairs.contains(new Pair<String, String>(otherStreamId, streamId))) {
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
    
    private void calculateCorrelations(Stream[] streams, IIFHayashiYoshidaPairwiseFinancialOutput pairwiseFinancialResult) {  // tuple for anchoring
        double correlation;
        boolean isFirst = true;
        IIFHayashiYoshidaPairwiseFinancialOutput further;
        for (int i = 0; i < streams.length; i++) {
            Stream s0 = streams[i];
            for (int j = i + 1; j < streams.length; j++) {
            	if (terminating) return;
                Stream s1 = streams[j];
    
                if (!streamPairs.contains(new Pair<String, String>(s0.getId(), s1.getId()))
                    && !streamPairs.contains(new Pair<String, String>(s1.getId(), s0.getId()))) {
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

                // If necessary flip the ids according to lexicographic order e.g. (b,a)->(a,b)
                String firstId = s0.getId().compareTo(s1.getId()) < 0 ? s0.getId() : s1.getId();
                String secondId = s0.getId().compareTo(s1.getId()) < 0 ? s1.getId() : s0.getId();
    
                String result = firstId + "," + secondId
                                + "," + dateFormat.format(new Date())
                                + "," + new DecimalFormat("0.000000").format(correlation);
        	    
                if (isFirst) {
                    pairwiseFinancialResult.setPairwiseCorrelationFinancial(result);
                    isFirst = false;
                } else {
                    further = pairwiseFinancialResult.addFurther();
                    further.setPairwiseCorrelationFinancial(result);
                }
            }
        }
    }
    
    private void shiftWindow(long windowStart, Stream[] streams) {
        for (int i = 0; i < streams.length; i++) {
        	if (terminating) return;
            Stream s0 = streams[i];
            for (Iterator<Interval> outerIt = s0.getIntervals().iterator(); outerIt.hasNext();) {
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
    
}
