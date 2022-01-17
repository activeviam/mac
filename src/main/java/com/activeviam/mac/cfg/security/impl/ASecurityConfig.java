/*
 * (C) ActiveViam 2017-2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.security.impl;

import com.activeviam.security.cfg.ICorsConfig;
import com.qfs.content.service.IContentService;
import com.qfs.jwt.service.IJwtService;
import com.qfs.server.cfg.IJwtConfig;
import com.qfs.server.cfg.impl.VersionServicesConfig;
import com.qfs.servlet.handlers.impl.NoRedirectLogoutSuccessHandler;
import com.quartetfs.biz.pivot.security.IAuthorityComparator;
import com.quartetfs.biz.pivot.security.impl.AuthorityComparatorAdapter;
import com.quartetfs.fwk.ordering.impl.CustomComparator;
import java.util.Collections;
import java.util.List;
import javax.servlet.Filter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.authentication.configuration.EnableGlobalAuthentication;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.context.SecurityContextPersistenceFilter;
import org.springframework.security.web.firewall.StrictHttpFirewall;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/**
 * Generic implementation for security configuration of a server hosting ActivePivot, or Content
 * server or ActiveMonitor.
 *
 * <p>This class contains methods:
 *
 * <ul>
 *   <li>To define authorized users,
 *   <li>To enable anonymous user access,
 *   <li>To configure the JWT filter,
 *   <li>To configure the security for Version service.
 * </ul>
 *
 * @author ActiveViam
 */
@EnableGlobalAuthentication
@Configuration
public abstract class ASecurityConfig implements ICorsConfig {

  /** Set to true to allow anonymous access. */
  public static final boolean useAnonymous = false;

  /** Authentication Bean Name. */
  public static final String BASIC_AUTH_BEAN_NAME = "basicAuthenticationEntryPoint";

  /** ActivePivot Cookie Name. */
  public static final String AP_COOKIE_NAME = "AP_JSESSIONID";

  /** Name of the User Role. */
  public static final String ROLE_USER = "ROLE_USER";

  /** Name of the Admin Role. */
  public static final String ROLE_ADMIN = "ROLE_ADMIN";

  /** Name of the Tech Role. */
  public static final String ROLE_TECH = "ROLE_TECH";

  /** Name of the ContentService Root Role. */
  public static final String ROLE_CS_ROOT = IContentService.ROLE_ROOT;

  /** The User Configuration. */
  @Autowired protected UserConfig userDetailsConfig;

  /** The JWT Configuration. */
  @Autowired protected IJwtConfig jwtConfig;

  /**
   * As of Spring Security 5.0, the way the passwords are encoded must be specified. When logging,
   * the input password will be encoded and compared with the stored encoded password. To determine
   * which encoding function was used to encode the password, the stored encoded passwords are
   * prefixed with the id of the encoding function.
   *
   * <p>In order to avoid reformatting existing passwords in databases one can set the default
   * <code>PasswordEncoder</code> to use for stored passwords that are not prefixed. This is the
   * role of the following function.
   *
   * @return The {@link PasswordEncoder} to encode passwords with.
   */
  @Bean
  @SuppressWarnings({"deprecation", "unused"})
  public PasswordEncoder passwordEncoder() {
    PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    ((DelegatingPasswordEncoder) passwordEncoder)
        .setDefaultPasswordEncoderForMatches(NoOpPasswordEncoder.getInstance());
    return passwordEncoder;
  }

  /**
   * Returns the default {@link AuthenticationEntryPoint} to use for the fallback basic HTTP
   * authentication.
   *
   * @return The default {@link AuthenticationEntryPoint} for the fallback HTTP basic
   *     authentication.
   */
  @Bean(name = BASIC_AUTH_BEAN_NAME)
  public AuthenticationEntryPoint basicAuthenticationEntryPoint() {
    return new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED);
  }

  /**
   * Configures the authentication of the whole application.
   *
   * <p>This binds the defined user service to the authentication and sets the source for JWT
   * tokens.
   *
   * @param auth Spring builder to manage authentication
   * @throws Exception in case of error
   */
  @Autowired
  public void configureGlobal(final AuthenticationManagerBuilder auth) throws Exception {
    auth.eraseCredentials(false)
        // Add an LDAP authentication provider instead of this to support LDAP
        .userDetailsService(this.userDetailsConfig.userDetailsService())
        .and()
        // Required to allow JWT
        .authenticationProvider(this.jwtConfig.jwtAuthenticationProvider());
  }

  /**
   * [Bean] Comparator for user roles.
   *
   * <p>Defines the comparator used by:
   *
   * <ul>
   *   <li>com.quartetfs.biz.pivot.security.impl.ContextValueManager#setAuthorityComparator(
   *       IAuthorityComparator)
   *   <li>{@link IJwtService}
   * </ul>
   *
   * @return a comparator that indicates which authority/role prevails over another.
   */
  @Bean
  public IAuthorityComparator authorityComparator() {
    final CustomComparator<String> comp = new CustomComparator<>();
    comp.setFirstObjects(Collections.singletonList(ROLE_USER));
    comp.setLastObjects(Collections.singletonList(ROLE_ADMIN));
    return new AuthorityComparatorAdapter(comp);
  }

  @Override
  public List<String> getAllowedOrigins() {
    return Collections.singletonList(CorsConfiguration.ALL);
  }

  /**
   * [Bean] Spring standard way of configuring CORS.
   *
   * <p>This simply forwards the configuration of {@link ICorsConfig} to Spring security system.
   *
   * @return the configuration for the application.
   */
  @Bean
  public CorsConfigurationSource corsConfigurationSource() {
    final CorsConfiguration configuration = new CorsConfiguration();
    configuration.setAllowedOrigins(getAllowedOrigins());
    configuration.setAllowedHeaders(getAllowedHeaders());
    configuration.setExposedHeaders(getExposedHeaders());
    configuration.setAllowedMethods(getAllowedMethods());
    configuration.setAllowCredentials(true);

    final UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", configuration);

    return source;
  }

  /**
   * Common configuration for {@link HttpSecurity}.
   *
   * @author ActiveViam
   */
  public abstract static class AWebSecurityConfigurer extends WebSecurityConfigurerAdapter {

    /** {@code true} to enable the logout URL. */
    protected final boolean logout;
    /** The name of the cookie to clear. */
    protected final String cookieName;

    /** The name of the Environment to use. */
    @Autowired protected Environment env;

    /** The ApplicationContext which contains the Beans. */
    @Autowired protected ApplicationContext context;

    /** This constructor does not enable the logout URL. */
    public AWebSecurityConfigurer() {
      this(null);
    }

    /**
     * This constructor enables the logout URL.
     *
     * @param cookieName the name of the cookie to clear
     */
    public AWebSecurityConfigurer(String cookieName) {
      this.logout = cookieName != null;
      this.cookieName = cookieName;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This configures a new firewall accepting `%` in URLs, as none of the core services encode
     * information in URL. This prevents from double-decoding exploits.<br>
     * The firewall is also configured to accept `\` - backslash - as none of ActiveViam APIs offer
     * to manipulate files from URL parameters.<br>
     * Yet, nor `/` and `.` - slash and point - are accepted, as it may trick the REGEXP matchers
     * used for security. Support for those two characters can be added at your own risk, by
     * extending this method. As far as ActiveViam APIs are concerned, `/` and `.` in URL parameters
     * do not represent any risk. `;` - semi-colon - is also not supported, for various APIs end up
     * target an actual database, and because this character is less likely to be used.
     */
    @Override
    public void configure(WebSecurity web) throws Exception {
      super.configure(web);

      final StrictHttpFirewall firewall = new StrictHttpFirewall();
      firewall.setAllowUrlEncodedPercent(true);
      firewall.setAllowBackSlash(true);

      firewall.setAllowUrlEncodedSlash(false);
      firewall.setAllowUrlEncodedPeriod(false);
      firewall.setAllowSemicolon(false);
      web.httpFirewall(firewall);
    }

    @Override
    protected final void configure(final HttpSecurity http) throws Exception {
      final Filter jwtFilter = this.context.getBean(IJwtConfig.class).jwtFilter();

      http
          // As of Spring Security 4.0, CSRF protection is enabled by default.
          .csrf()
          .disable()
          .cors()
          .and()
          // To allow authentication with JWT (Required for ActiveUI)
          .addFilterAfter(jwtFilter, SecurityContextPersistenceFilter.class);

      if (this.logout) {
        // Configure logout URL
        http.logout()
            .permitAll()
            .deleteCookies(this.cookieName)
            .invalidateHttpSession(true)
            .logoutSuccessHandler(new NoRedirectLogoutSuccessHandler());
      }

      if (useAnonymous) {
        // Handle anonymous users. The granted authority ROLE_USER
        // will be assigned to the anonymous request
        http.anonymous().principal("guest").authorities(ROLE_USER);
      }

      doConfigure(http);
    }

    /**
     * Applies the specific configuration for the endpoint.
     *
     * @param http the http endpoint to configure.
     * @throws Exception in case of error.
     * @see #configure(HttpSecurity)
     */
    protected abstract void doConfigure(HttpSecurity http) throws Exception;
  }

  /**
   * Configuration for Version service to allow anyone to access this service.
   *
   * @author ActiveViam
   * @see HttpStatusEntryPoint
   */
  public abstract static class AVersionSecurityConfigurer extends WebSecurityConfigurerAdapter {

    /** The autowired Spring context. */
    @Autowired protected ApplicationContext context;

    @Override
    protected void configure(HttpSecurity http) throws Exception {

      http.antMatcher(VersionServicesConfig.REST_API_URL_PREFIX + "/**")
          // As of Spring Security 4.0, CSRF protection is enabled by default.
          .csrf()
          .disable()
          .cors()
          .and()
          .authorizeRequests()
          .antMatchers("/**")
          .permitAll();
    }
  }
}
