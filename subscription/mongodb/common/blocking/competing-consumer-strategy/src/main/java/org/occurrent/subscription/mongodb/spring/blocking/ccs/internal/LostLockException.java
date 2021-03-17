/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package org.occurrent.subscription.mongodb.spring.blocking.ccs.internal;

public class LostLockException extends RuntimeException {
  public LostLockException(String message) {
    super(message);
  }

  public LostLockException(String resource, String listenerId) {
    super(listenerId + " lost lock to " + resource);
  }
}
