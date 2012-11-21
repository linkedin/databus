package com.linkedin.databus.core.data_model;

import java.net.URI;

import com.linkedin.databus2.core.DatabusException;

/**
 * An interface for {@link DatabusSubscription} URI codec.
 */
public interface SubscriptionUriCodec
{
  /** The unique prefix (e.g. oracle or espresso) for this codec.
   * Note: it must not include a colon.  */
  String getScheme();

  /**
   * Attempts to parse the specified URI into a {@link DatabusSubscription} object
   * @return the resulting subscription object
   */
  DatabusSubscription decode(URI uri) throws DatabusException;

  /** Creates a URI from a subscription */
  URI encode(DatabusSubscription sub);
}
