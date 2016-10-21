package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.base.algorithm.IFamily;
import eu.qualimaster.common.signal.ParameterChangeSignal;
import eu.qualimaster.common.signal.SignalException;
import eu.qualimaster.observables.IObservable;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by Apostolos Nydriotis on 2015/08/03.
 */
public abstract class AbstractSubTopology {

  public final static String TERMINATION_PARAMETER = "you are done";

  protected final static Logger logger = Logger.getLogger(AbstractSubTopology.class);
  protected String windowSizeHandlerExecutorName;
  protected String windowSizeHandlerExecutorNamespace;

  protected List<String> terminationHandlersNames;
  protected List<String> terminationHandlersNameSpaces;

  public void setParameterWindowSize(int i) {

    logger.info("setParameterWindowSize");

    try {
      logger.info("sending new windowSize signal " + i + "!");
      ParameterChangeSignal parameterChangeSignal =
          new ParameterChangeSignal(windowSizeHandlerExecutorNamespace,
                                    windowSizeHandlerExecutorName,
                                    "windowSize", i, "");
      parameterChangeSignal.sendSignal();
    } catch (Exception e) {
      logger.error("Signal not sent!");
      e.printStackTrace();
    }
  }

  public void switchState(IFamily.State state) {
    // TODO(anydriotis): Handle State.TERMINATING here (mapper & HY)
    if (state == IFamily.State.TERMINATING) {
      logger.info("sending termination signals");
      for (int i = 0; i < terminationHandlersNames.size(); i++) {
        try {
          ParameterChangeSignal parameterChangeSignal =
              new ParameterChangeSignal(terminationHandlersNameSpaces.get(i),
                                        terminationHandlersNames.get(i),
                                        TERMINATION_PARAMETER, "", "");
          parameterChangeSignal.sendSignal();
        } catch (SignalException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
