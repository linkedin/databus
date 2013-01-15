package com.linkedin.databus.core;

/** Describes the type of a {@link Checkpoint}. */
public enum DbusClientMode
{
  /** An empty checkpoint */
  INIT,
  /** A bootstrap checkpoint in the Snapshot phase */
  BOOTSTRAP_SNAPSHOT,
  /** A bootstrap checkpoint in the Catchup phase */
  BOOTSTRAP_CATCHUP,
  /** A checkpoint for consuming from a relay */
  ONLINE_CONSUMPTION
}
