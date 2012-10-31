package com.linkedin.databus.core.data_model;

import java.net.URI;
import java.net.URISyntaxException;

import com.linkedin.databus2.core.DatabusException;

/**
 * A codec for the legacy string representation of subscriptions
 * The format is either:
 * <ul>
 *      <li>LogicalSourceName for Databus V2 sources, or
 *      <li>EspressoDBName.TableName:PartitionNum for Espresso V3 sources
 * </ul>
 */
public class LegacySubscriptionUriCodec implements SubscriptionUriCodec
{
  private static final LegacySubscriptionUriCodec THE_INSTANCE = new LegacySubscriptionUriCodec();

  public static LegacySubscriptionUriCodec getInstance()
  {
    return THE_INSTANCE;
  }

  @Override
  public String getScheme()
  {
    return "legacy";
  }

  @Override
  public DatabusSubscription decode(URI uri) throws DatabusException
  {
    String scheme = uri.getScheme();
    String s = getScheme().equals(scheme) ? uri.getSchemeSpecificPart() : uri.toString();

    return DatabusSubscription.createSimpleSourceSubscription(s);
  }

  @Override
  public URI encode(DatabusSubscription sub)
  {
    try
    {
      String uriSsc = DatabusSubscription.createStringFromSubscription(sub);
      URI result = uriSsc.indexOf(':') >=0 ? new URI(getScheme(), uriSsc, null) : new URI(uriSsc);
      return result;
    }
    catch (URISyntaxException e)
    {
      throw new RuntimeException("unable to generate legacy subscription URI: " + e.getMessage(), e);
    }
  }

}
