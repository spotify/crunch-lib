package com.spotify.crunch.lib;

/**
 * An exception which will be thrown when the Crunch job gets planned because of some kind of invalid setup relating to
 * a non-typesafe operation (such as SPCollections.keyByAvroField)
 */
public class PlanTimeException extends RuntimeException {
  public PlanTimeException(String message) { super(message); }
  public PlanTimeException(String message, Exception cause) { super(message, cause); }
}
