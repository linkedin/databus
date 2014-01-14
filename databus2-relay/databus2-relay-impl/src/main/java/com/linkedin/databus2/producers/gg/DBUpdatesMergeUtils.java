package com.linkedin.databus2.producers.gg;
/*
*
* Copyright 2013 LinkedIn Corp. All rights reserved
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.linkedin.databus2.ggParser.XmlStateMachine.ColumnsState;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState.DBUpdateImage;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState.PerSourceTransactionalUpdate;

public class DBUpdatesMergeUtils
{
  /**
   * Merge 2 list of DBUpdates and return the merged list. The ordering of
   * PerSourceTransactionalUpdates ( in the order of SourceIds) is guaranteed in the
   * output list If two events have the same sourceId and same key, the event present in
   * newDbUpdates is the winner and will be present in the output
   *
   * @param newTxnDbUpdates
   *          : New DbUpdates list to be merged
   * @param oldDbUpdates
   *          : Old DbUpdates list to be merged.
   * @return Merged list
   */
  public static List<TransactionState.PerSourceTransactionalUpdate> mergeTransactionData(List<TransactionState.PerSourceTransactionalUpdate> newTxnDbUpdates,
                                                                                         List<TransactionState.PerSourceTransactionalUpdate> oldDbUpdates)
  {

    if ((null == oldDbUpdates) && (null == newTxnDbUpdates))
    {
      return null;
    }

    Map<Integer, TransactionState.PerSourceTransactionalUpdate> combinedTxnDbUpdatesMap =
        new HashMap<Integer, TransactionState.PerSourceTransactionalUpdate>();

    if (null != oldDbUpdates)
    {
      for (TransactionState.PerSourceTransactionalUpdate dbu : oldDbUpdates)
      {
        combinedTxnDbUpdatesMap.put(dbu.getSourceId(), dbu);
      }
    }

    if (null != newTxnDbUpdates)
    {
      for (TransactionState.PerSourceTransactionalUpdate dbu : newTxnDbUpdates)
      {
        TransactionState.PerSourceTransactionalUpdate oldDbu =
            combinedTxnDbUpdatesMap.get(dbu.getSourceId());

        if (null == oldDbu)
        {
          // No old Entry present. We can just copy the newDbUpdate's entry to
          // combinedTxnDbbUpdatesMap
          combinedTxnDbUpdatesMap.put(dbu.getSourceId(), dbu);
        }
        else
        {
          // Do the merge
          Set<DbUpdateState.DBUpdateImage> mergeUpdateImages =
              mergeDbUpdates(dbu.getDbUpdatesSet(), oldDbu.getDbUpdatesSet());
          TransactionState.PerSourceTransactionalUpdate mergedPerSourceTxnUpdate =
              new TransactionState.PerSourceTransactionalUpdate(dbu.getSourceId(),
                                                                mergeUpdateImages);
          combinedTxnDbUpdatesMap.put(dbu.getSourceId(), mergedPerSourceTxnUpdate);
        }
      }
    }

    List<PerSourceTransactionalUpdate> sourceTransactionalUpdates =
        new ArrayList<PerSourceTransactionalUpdate>(combinedTxnDbUpdatesMap.size());
    for (Entry<Integer, PerSourceTransactionalUpdate> entry : combinedTxnDbUpdatesMap.entrySet())
    {
      sourceTransactionalUpdates.add(entry.getValue());
    }

    // Sort by source id to make sure the window has event batches sorted by sourceIds
    Collections.sort(sourceTransactionalUpdates);

    return sourceTransactionalUpdates;
  }

  private static Set<DbUpdateState.DBUpdateImage> mergeDbUpdates(Set<DbUpdateState.DBUpdateImage> newDbUpdateImage,
                                                                 Set<DbUpdateState.DBUpdateImage> oldDbUpdateImage)
  {
    Map<List<ColumnsState.KeyPair>, DbUpdateState.DBUpdateImage> result =
        new HashMap<List<ColumnsState.KeyPair>, DbUpdateState.DBUpdateImage>();

    for (DBUpdateImage dbi : oldDbUpdateImage)
    {
      result.put(dbi.getKeyPairs(), dbi);
    }

    // If duplicate entries are present, they will be overwritten with entries from
    // newDbUpdateImage
    for (DBUpdateImage dbi : newDbUpdateImage)
    {
      result.put(dbi.getKeyPairs(), dbi);
    }

    return new HashSet<DbUpdateState.DBUpdateImage>(result.values());
  }
}
