package com.linkedin.databus3.espresso.client.test;

import java.io.Serializable;
import org.apache.log4j.Logger;

/**
 * Represents a unique Sequence Number for Espresso.
 *
 * @author aauradka, lqiao
 */
public class EspressoSCN implements Serializable, Comparable<EspressoSCN>
{
  /**
   * Compatibility Checking
   */
  private static final long serialVersionUID = -7740877498386503973L;
  private final long _scn;

  public static final Logger LOG = Logger.getLogger(EspressoSCN.class.getName());
  public static final int INVALID_GENERATION = -1; // Default error generation number
  public static final int START_GENERATION = 1;  // The starting generation number when a db/partition is created
  public static final int MAX_GENERATION = 0x7FFFFFFF; // Max generation number
  public static final int INVALID_SEQUENCE = -1; // Default error sequence number
  public static final int START_SEQUENCE = 1;  // The starting sequence number when a db/partition is created
  public static final int MAX_SEQUENCE = 0x7FFFFFFF; // Max sequence number

  // This pair is used for scn when an index is empty and used for query to satisfy read-after-write in all occasions.
  public static final int EMPTY_GENERATION = 0;
  public static final int EMPTY_SEQUENCE = 0;

  /**
   * Used to indicate an invalid SCN.
   */
  public static final EspressoSCN INVALID_SCN = new EspressoSCN(INVALID_GENERATION, INVALID_SEQUENCE);
  /**
   * The very first SCN.
   */
  public static final EspressoSCN START_SCN = new EspressoSCN(START_GENERATION, START_SEQUENCE);
  /**
   * The default query SCN to satisfy read-after-write no matter what
   */
  public static final EspressoSCN EMPTY_SCN = new EspressoSCN(EMPTY_GENERATION, EMPTY_SEQUENCE);

  private static final long GenerationMask = 0xFFFFFFFF00000000L;
  private static final long SequenceMask =   0x00000000FFFFFFFFL;

  public EspressoSCN(int generation, int sequence)
  {
    _scn = (((long) generation  << 32) & GenerationMask) | ((long) sequence & SequenceMask);
  }

  public EspressoSCN(long scn)
  {
    _scn = scn;
  }

  public int getGeneration()
  {
    return (int) ((_scn & GenerationMask) >> 32);
  }

  public int getSequence()
  {
    return (int) (_scn & SequenceMask);
  }

  public long getSCN()
  {
    return _scn;
  }

  /*
   * Do not rename to getNextSCN(). This is not a real member variable getter.
   * This throws Jackson off, and throws a JsonMappingException.
   */
  public EspressoSCN nextSCN()
  {
    int generation = getGeneration();
    int sequence = getSequence();
    if(sequence == MAX_SEQUENCE)
    {
      if(generation == MAX_GENERATION)
      {
        throw new  IllegalStateException("Generation already maxed, cannot increase sequence any more.");
      }
      else
      {
        generation++;
        sequence = START_SEQUENCE;
      }
    }
    else
    {
      sequence++;
    }

    return new EspressoSCN(generation, sequence);
  }

  /*
   * @param scnString is in the same format as the string returned by toString()
   */
  public static EspressoSCN parseSCN(String scnString)
  {
    String tmp[] = scnString.split("_");
    if(tmp.length != 4)
    {
      throw new IllegalArgumentException(scnString + " is not in the correct format");
    }

    return new EspressoSCN(Integer.parseInt(tmp[1]), Integer.parseInt(tmp[3]));
  }

  public String toString()
  {
    // Note that the following format cannot be changed easily since we have backup
    // artifacts that depend on EspressoSCNs serializing
    // in the "generation_1_sequence_2345" format. Please talk to Oliver if you need to
    // change this.
    return "generation_" + getGeneration() + "_sequence_" + getSequence();
  }

  /** Check if this is a valid EspressoSCN
   *  @return true iff this is valid
   */
  public boolean isValidSCN()
  {
    return (isValidGeneration() && isValidSequence());
  }

  /** Check if this is a valid generation
   *  @return true iff this is valid
   */
  public boolean isValidGeneration()
  {
    // Check if generation is bigger than the start_generation and smaller than max_generation
    boolean isValid = ((getGeneration()>=START_GENERATION) && (getGeneration()<=MAX_GENERATION));
    if (isValid != true)
    {
       LOG.warn("This EspressoSCN(" + getGeneration() + "," + getSequence() + ") has invalid generation");
    }

    return isValid;
  }

  /** Check if this is a valid sequence number
   *  @return true iff this is valid
   */
  public boolean isValidSequence()
  {
    // Check if sequence is bigger than the start_sequence and smaller than max_sequence
    boolean isValid = ((getSequence()>=START_SEQUENCE) && (getSequence()<=MAX_SEQUENCE));
    if (isValid != true)
    {
       LOG.warn("This EspressoSCN(" + getGeneration() + "," + getSequence() + ") has invalid sequence!");
    }

    return isValid;

  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof EspressoSCN)
    {
      EspressoSCN scn = (EspressoSCN) o;
      if (scn.getSCN() == this._scn)
      {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    // Returning the sequence number as hashCode
    return (int) _scn;
  }

  /**
   * We do have the case when the INVALID_SCN (-1, -1) is compared with regular scn. Otherwise we should allow the
   * compare of SCN with negative gen or seq number.
   */
  private boolean isComparable()
  {
    return this.equals(INVALID_SCN) || (getGeneration() >= 0 && getSequence() >= 0);
  }

  @Override
  public int compareTo(EspressoSCN that)
  {
    if (that == null)
    {
      throw new IllegalArgumentException("Cannot compare a null scn");
    }
    // allow equals of any invalid scn
    if (this == that || this.equals(that))
    {
      return 0;
    }

    // Do not allow larger or smaller than an invalid scn
    if(!this.isComparable() || !that.isComparable())
    {
      throw new IllegalArgumentException("Connot compare invalid scn for " + this + " and " + that);
    }
    if (this.getSCN() > that.getSCN())
    {
      return 1;
    }
    else
    {
      return -1;
    }
  }

}
