/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.qfs.desc.IStorePermission;
import com.qfs.desc.IStoreSecurity;
import com.qfs.service.store.impl.ADatastoreServiceConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.springframework.context.annotation.Configuration;

/** Configuration preventing any edition to the datastore from the remote services. */
@Configuration
public class NoWriteDatastoreServiceConfig extends ADatastoreServiceConfig {

  /** Constructor. */
  public NoWriteDatastoreServiceConfig() {
    super(Collections.emptyMap(), Collections.emptyMap(), storesSecurityMap, DEFAULT_QUERY_TIMEOUT);
  }

  /**
   * A constant for the map which will always return the same store security, also defined as a
   * constant below.
   */
  protected static final Map<String, IStoreSecurity> storesSecurityMap =
      new HashMap<>() {
        private static final long serialVersionUID = 5_08_00L;

        @Override
        public IStoreSecurity get(Object key) {
          return containsKey(key) ? super.get(key) : defaultStoreSecurity;
        }
      };

  /**
   * A constant for store security.
   *
   * <p>This allows to read any fields but forbids all updates.
   */
  protected static final IStoreSecurity defaultStoreSecurity =
      new IStoreSecurity() {

        @Override
        public boolean isDeletingRecordsAllowed() {
          return false;
        }

        @Override
        public boolean isAddingNewRecordsAllowed() {
          return false;
        }

        @Override
        public IStorePermission getStorePermissions() {
          return new IStorePermission() {

            @Override
            public Set<String> getStoreWriterRoles() {
              return Collections.emptySet();
            }

            @Override
            public Set<String> getStoreReaderRoles() {
              return Collections.emptySet();
            }

            @Override
            public boolean canWriteField(String field, Set<String> roles) {
              return false;
            }

            @Override
            public boolean canReadField(String field, Set<String> roles) {
              return true;
            }
          };
        }
      };
}
