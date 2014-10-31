/*
 * Copyright 2014 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.crunch.lib;

/**
 * An exception which will be thrown when the Crunch job gets planned because of some kind of invalid setup relating to
 * a non-typesafe operation (such as SPCollections.keyByAvroField)
 */
public class PlanTimeException extends RuntimeException {
  public PlanTimeException(String message) { super(message); }
  public PlanTimeException(String message, Exception cause) { super(message, cause); }
}
