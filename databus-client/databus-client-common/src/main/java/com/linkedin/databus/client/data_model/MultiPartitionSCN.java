package com.linkedin.databus.client.data_model;

import java.util.Arrays;

import com.linkedin.databus.client.pub.SCN;

/** A wrapper for a checkpoint that implements the SCN interface */
public class MultiPartitionSCN implements SCN
{

  private static final long serialVersionUID = 1L;
  private long[] _seq;

  public MultiPartitionSCN(int numPartitions)
  {
    _seq = new long[numPartitions];
  }

  /** Copy constructor */
  public MultiPartitionSCN(MultiPartitionSCN other)
  {
    _seq = other.getSequences();
  }

  /** Create from a different SCN with modifying a single partition sequence */
  public MultiPartitionSCN(MultiPartitionSCN other, int partitionIdx, long seq)
  {
    _seq = other.getSequences(partitionIdx < other._seq.length ? null : new long[partitionIdx + 1]);
    _seq[partitionIdx] = seq;
  }

  @Override
  public boolean isComparable(SCN otherSCN)
  {
    if (null == otherSCN || ! (otherSCN instanceof MultiPartitionSCN)) return false;
    MultiPartitionSCN other = (MultiPartitionSCN)otherSCN;
    return _seq.length == other._seq.length;
  }

  @Override
  public int compareTo(SCN otherSCN)
  {
    //not comparable
    if (null == otherSCN) throw new NullPointerException("can't compare to a null SCN");
    if (! (otherSCN instanceof MultiPartitionSCN)) return 0;
    MultiPartitionSCN other = (MultiPartitionSCN)otherSCN;
    //not comparable
    if (_seq.length != other._seq.length) return 0; //incomparable
    int i = 0;
    for (; i < _seq.length && _seq[i] == other._seq[i]; ++i);
    if (i == _seq.length) return 0;

    return _seq[i] < other._seq[i] ? -1 : 1;
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || ! (other instanceof SCN)) return false;
    SCN otherSCN = (SCN)other;
    return isComparable(otherSCN) && 0 == compareTo(otherSCN);
  }

  @Override
  public int hashCode()
  {
    return (null == _seq) ? 0 : Arrays.hashCode(_seq);
  }

  /**
   * Copies the per-partition sequence numbers to an array. This method will make a copy of the
   * numbers.
   * @return the populated array with per-partition sequence numbers
   */
  public long[] getSequences()
  {
    return getSequences(null);
  }

  /**
   * Copies the per-partition sequence numbers to an array. If the passed array is null or it has
   * insufficient length, a new array will be allocated. If the passed array has more elements
   * than necessary, only the first elements will be modified
   * @param reuse       a previously allocated array to try to reuse
   * @return the populated array with per-partition sequence numbers
   */
  public long[] getSequences(long[] reuse)
  {
    if (null == reuse || reuse.length < _seq.length) reuse = new long[_seq.length];
    System.arraycopy(_seq, 0, reuse, 0, _seq.length);

    return reuse;
  }

  public int size()
  {
    return _seq.length;
  }

  /** Obtains the sequence number of a partition with a given index */
  public long partitionSeq(int partitionIdx)
  {
    return _seq[partitionIdx];
  }

  /** Changes the sequence for a given partition*/
  public void partitionSeq(int partitionIdx, long seq)
  {
    if (partitionIdx >= _seq.length) _seq = getSequences(new long[partitionIdx + 1]);
    _seq[partitionIdx] = seq;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(_seq.length * 50);
    sb.append('[');
    boolean first = true;
    for (long i: _seq)
    {
      if (first) first = false;
      else sb.append(',');
      sb.append(i);
    }
    sb.append(']');

    return sb.toString();
  }

}
