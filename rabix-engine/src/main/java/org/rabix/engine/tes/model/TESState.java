package org.rabix.engine.tes.model;

public enum TESState {
  Unknown,
  Queued,
  Running,
  Paused,
  Complete,
  Error,
  SystemError,
  Canceled,
  Initializing
}
