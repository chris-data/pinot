package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchAutoTuneResult {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAutoTuneResult.class);

  private static final String CSVSUFFIX = ".csv";
  private static final Integer DEFAULT_NEXPECTEDANOMALIES = 3;

  /**
   * Tool to fetch auto tune results using auto tune endpoint based on collection;
   * evaluate alert filter using evaluation endpoint
   * @param args
   * args[0]: Type of AutoTune, default value is: AUTOTUNE
   * args[1]: Start time of merged anomaly in milliseconds
   * args[2]: End time of merged anomaly in milliseconds
   * args[3]: Path to Persistence File
   * args[4]: Collection Name
   * args[6]: File directory (read and write file)
   * args[7]: Holiday periods to remove from dataset. Holiday starts in format: milliseconds1,milliseconds2
   * args[8]: Holiday periods to remove from dataset. Holiday ends in format: milliseconds1,milliseconds2
   * @throws Exception
   */
  public static void main(String[] args) throws Exception{

    if(args.length < 8){
      LOG.error("Error: Insufficient number of arguments", new IllegalArgumentException());
      return;
    }

    String AUTOTUNE_TYPE = args[0];
    Long STARTTIME = Long.valueOf(args[1]);
    Long ENDTIME = Long.valueOf(args[2]);
    String path2PersistenceFile = args[3];
    String Collection = args[4];
    String DIR_TO_FILE = args[5];
    String holidayStarts = args[6];
    String holidayEnds = args[7];

    AutoTuneAlertFilterTool executor = new AutoTuneAlertFilterTool(new File(path2PersistenceFile));

    List<Long> functionIds = executor.getAllFunctionIdsByCollection(Collection);
    Map<String, String> outputMap = new HashMap<>();
    for(Long functionId : functionIds){
      StringBuilder outputVal = new StringBuilder();

      // evaluate current alert filter
      String origEvals = executor.evalAnomalyFunctionAlertFilterToEvalNode(functionId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();

      // evaluate labels, if has no labels, go to initiate alert filter, else go to tune alert filter
      Long autotuneId;
      Boolean isInitAutoTune = !executor.checkAnomaliesHasLabels(functionId, STARTTIME, ENDTIME, holidayStarts, holidayEnds);

      if(isInitAutoTune){
        autotuneId = Long.valueOf(executor.getInitAutoTuneByFunctionId(functionId, STARTTIME, ENDTIME, AUTOTUNE_TYPE, DEFAULT_NEXPECTEDANOMALIES, holidayStarts, holidayEnds));
      } else{
        // tune by functionId
        autotuneId = Long.valueOf(executor.getTunedAlertFilterByFunctionId(functionId, STARTTIME, ENDTIME, AUTOTUNE_TYPE, holidayStarts, holidayEnds));
      }

      boolean isUpdated = autotuneId != -1;
      String tunedEvals = "";
      if(isUpdated){
        // after tuning, evaluate tuned results by autotuneId
        tunedEvals = executor.evalAutoTunedAlertFilterToEvalNode(autotuneId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();
      }

      outputVal.append(origEvals)
          .append(isUpdated)
          .append(",")
          .append(tunedEvals);

      outputMap.put(String.valueOf(functionId), outputVal.toString());
    }

    // write to file
    String header = "FunctionId" + "," + AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema() + "," + "isModelUpdated" + "," +AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema();
    executor.writeMapToCSV(outputMap, DIR_TO_FILE + Collection + CSVSUFFIX, header);
    LOG.info("Write evaluations to file: {}", DIR_TO_FILE + Collection + CSVSUFFIX);
  }
}
