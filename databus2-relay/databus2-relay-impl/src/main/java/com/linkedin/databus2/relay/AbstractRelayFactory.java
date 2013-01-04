package com.linkedin.databus2.relay;

import com.linkedin.databus2.schemas.SourceIdNameRegistry;

/** Common functionality for Relay factories */
public abstract class AbstractRelayFactory implements RelayFactory
{
  private final SourceIdNameRegistry _sourcesIdNameRegistry;

  public AbstractRelayFactory()
  {
    this(new SourceIdNameRegistry());
  }

  public AbstractRelayFactory(SourceIdNameRegistry sourcesIdNameRegistry)
  {
    if(sourcesIdNameRegistry == null)
      sourcesIdNameRegistry = new SourceIdNameRegistry();
    _sourcesIdNameRegistry = sourcesIdNameRegistry;
  }

  public SourceIdNameRegistry getSourcesIdNameRegistry()
  {
    return _sourcesIdNameRegistry;
  }

}
