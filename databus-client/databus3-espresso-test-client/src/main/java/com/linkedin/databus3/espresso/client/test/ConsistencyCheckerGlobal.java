package com.linkedin.databus3.espresso.client.test;

import org.apache.log4j.Logger;





public class ConsistencyCheckerGlobal
{
  private final Partition _partition;
  private static final Logger LOG                = Logger.getLogger(ConsistencyChecker.class.getName()); 
  private MasterHighwaterMarkStore _masterHwmStore;  
  
  public ConsistencyCheckerGlobal(Partition partition, MasterHighwaterMarkStore masterHwmStore)
  {
    _partition = partition;
    _masterHwmStore = masterHwmStore;
  }

  /**
   * Check if sequence for local gen <= than global end sequence for the generation
   * Also checks for gen in [startGlobalGen, endGlobalGen) that
   * if gen matches local generation that sequence numbers match exactly or if gen is different it is empty
   * @throws Exception iff the SCNs are not consistent
   */
  public void checkConsistency(EspressoSCN localSCN, int startGlobalGen, int endGlobalGen) throws Exception
  {

    if (!localSCN.isValidSCN())
    {
      String msg =
          String.format("For partition %s, invalid local SCN HWM:%s",
                        _partition.toString(),
                        localSCN.toString());
      throw new Exception(msg);
    }

    checkConsistency(localSCN);

    for(int gen = startGlobalGen; gen < endGlobalGen; gen++)
    {
      EspressoSCN scnForGenerationEnd = _masterHwmStore.getSCNForGenerationEnd(_partition, gen);
      if(scnForGenerationEnd != null && scnForGenerationEnd.getGeneration() == localSCN.getGeneration()
          && scnForGenerationEnd.getSequence() != localSCN.getSequence())
      {
        String msg =
            "Detected inconsistency in partition " + _partition.toString()
                + " Local sequence " + localSCN.getSequence() + " for generation "
                + localSCN.getGeneration()
                + " does not match the recorded global sequence: "
                + scnForGenerationEnd.getSequence();
        throw new Exception(msg);
      }
      else if(scnForGenerationEnd != null && scnForGenerationEnd.getGeneration() != localSCN.getGeneration())
      {
        //we've not seen this generation - verify its empty
        EspressoSCN scnForGenerationStart = _masterHwmStore.getSCNForGenerationStart(_partition, gen);
        if(scnForGenerationStart != null && scnForGenerationStart.getSequence() != scnForGenerationEnd.getSequence())
        {
          String msg =
              "Detected inconsistency in partition " + _partition.toString()
                  + " Global generation " + gen + " is not empty. "
                  + "Start sequence = " + scnForGenerationStart.getSequence()
                  + " End sequence =  " + scnForGenerationEnd.getSequence();
          throw new Exception(msg);
        }
      }
    }
  }

  /**
   *
   * @param localSCN the SCN which needs to be validated
   * @throws Exception if localSCN's gen > global gen or if generations match and local seq > global seq
   */
  public void checkConsistency(EspressoSCN localSCN) throws Exception
  {
    EspressoSCN globalSCN = _masterHwmStore.getLatestSCN(_partition);
    if(globalSCN != null && localSCN.getGeneration() > globalSCN.getGeneration())
    {
      String msg =
          "Detected inconsistency in partition " + _partition.toString()
              + " Local generation " + localSCN.getGeneration()
              + " exceeds the recorded global generation: "
              + globalSCN.getGeneration();
      LOG.error(msg);
      throw new Exception(msg);
    }

    EspressoSCN globalGenEndSCNForLocalGen = _masterHwmStore.getSCNForGenerationEnd(_partition, localSCN.getGeneration());
    if(globalGenEndSCNForLocalGen != null && localSCN.getSequence() > globalGenEndSCNForLocalGen.getSequence())
    {
      String msg =
          "Detected inconsistency in partition " + _partition.toString()
              + " Local sequence " + localSCN.getSequence() + " for generation "
              + localSCN.getGeneration() + " exceeds the recorded global sequence: "
              + globalGenEndSCNForLocalGen.getSequence();
      LOG.error(msg);
      throw new Exception(msg);
    }
  }

  /**
   * Basic logic:
   * Skip to the beginning of the next generation until:
   * (1) The scn generation is already the global generation.
   * (2) The scn sequence is smaller than the sequence of the global max sequence for this generation.
   */
  public EspressoSCN skipEmptySCNRange(EspressoSCN startSCN) throws Exception
  {
    EspressoSCN globalLatestSCN =  _masterHwmStore.getLatestSCN(_partition);
    if(startSCN == null || globalLatestSCN == null)
    {
      return startSCN;
    }

    while(startSCN.getGeneration() < globalLatestSCN.getGeneration())
    {
      EspressoSCN scnForGenerationEnd = _masterHwmStore.getSCNForGenerationEnd(_partition, startSCN.getGeneration());
      if(scnForGenerationEnd == null || startSCN.getSequence() < scnForGenerationEnd.getSequence())
      {
        break;
      }
      EspressoSCN nextGenSCN = new EspressoSCN(startSCN.getGeneration() + 1, EspressoSCN.START_SEQUENCE);
      startSCN = nextGenSCN;
    }
    return startSCN;
  }
}
