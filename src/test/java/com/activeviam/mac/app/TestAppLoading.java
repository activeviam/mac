/*
 * (C) ActiveViam 2023
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.app;

import com.activeviam.mac.cfg.impl.MacServerConfig;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

/** Test class for tests related to loading cases of the SpringBoot application */
@WebAppConfiguration("classpath:application.yml")
@ContextConfiguration(classes = MacServerConfig.class)
public class TestAppLoading {

  /** Ensures that the default application setup works */
  @Test
  public void loadFromCorrectPath() {
    Assertions.assertDoesNotThrow(() -> MacSpringBootApp.main(new String[] {}));
  }

  /**
   * Ensures that a customized application setup with an incorrect path fails to instantiate the
   * application.
   */
  @Test
  public void loadFailureFromIncorrectPath() {
    final BeanCreationException thrown =
        Assertions.assertThrows(
            BeanCreationException.class,
            () -> MacSpringBootApp.main(new String[] {"--statistic.folder=FAKE"}));
    final Throwable cause = thrown.getRootCause();
    assert cause != null;
    Assertions.assertEquals(cause.getClass(), ActiveViamRuntimeException.class);
    Assertions.assertTrue(
        cause.getMessage().contains("is not a correct path to a valid directory"));
  }
}
