package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.families.inf.IFPreprocessor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by Apostolos Nydriotis on 12/10/14.
 */
public class Preprocessor implements IFPreprocessor {

  Logger logger = Logger.getLogger(Preprocessor.class);
  private boolean terminating;

  public Preprocessor() {
    terminating = false;
  }

  @Override
  public void switchState(State state) {
    if (state == State.TERMINATING) {
      terminating = true;
    }
  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  @Override
  public void calculate(
      IIFPreprocessorSpringStreamInput iifPreprocessorSpringStreamInput,
      IIFPreprocessorPreprocessedStreamOutput iifPreprocessorPreprocessedStreamOutput)
      throws DefaultModeException {

    iifPreprocessorPreprocessedStreamOutput.clear();

    if (terminating) return;

    String symbolData = iifPreprocessorSpringStreamInput.getSymbolTuple();
    String[] fields = symbolData.split(",");

    String id = fields[0];

    String date = fields[1] + " " + fields[2];
    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss");
    long timestamp = 0;
    try {
      timestamp = dateFormat.parse(date).getTime();
    } catch (ParseException e) {
      logger.error("Wrong data parsed (date)! date: " + date + " got " + symbolData, e);
      throw new DefaultModeException("Wrong data parsed (date)! " + e.getMessage()
                                     + "date: " + date + " got " + symbolData);
    }

    double value;
    int volume;

    try {
      value = Double.parseDouble(fields[3]);
    } catch (Exception e) {
      logger.error("Wrong data parsed (value)! value: " + fields[3] + " got " + symbolData, e);
      throw new DefaultModeException(
          "Wrong data parsed (value)! " + e.getMessage() + " value: " + fields[3] + " got " + symbolData);
    }

    try {
      volume = Integer.parseInt(fields[4]);
    } catch (Exception e) {
      logger.error("Wrong data parsed (volume)! volume: " + fields[4] + " got " + symbolData, e);
      logger.error("sending volume = 0");
      volume = 0;
//      throw new DefaultModeException(
//          "Wrong data parsed (volume)! " + e.getMessage() + " volume: "
//          + fields[4] + " got " + symbolData);
    }

    iifPreprocessorPreprocessedStreamOutput.setSymbolId(id);
    iifPreprocessorPreprocessedStreamOutput.setTimestamp(timestamp);
    iifPreprocessorPreprocessedStreamOutput.setValue(value);
    iifPreprocessorPreprocessedStreamOutput.setVolume(volume);
  }
}
