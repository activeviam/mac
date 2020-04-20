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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.Filter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
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
 * <ul>
 * <li>To define authorized users</li>,
 * <li>To enable anonymous user access</li>,
 * <li>To configure the JWT filter</li>,
 * <li>To configure the security for Version service</li>.
 * </ul>
 *
 * @author ActiveViam
 */
@EnableGlobalAuthentication
@Configuration
public abstract class ASecurityConfig implements ICorsConfig {

  /** Set to true to allow anonymous access. */
  public static final boolean useAnonymous = false;

  public static final String BASIC_AUTH_BEAN_NAME = "basicAuthenticationEntryPoint";

  public static final String AP_COOKIE_NAME = "AP_JSESSIONID";

  public static final String ROLE_USER = "ROLE_USER";
  public static final String ROLE_ADMIN = "ROLE_ADMIN";
  public static final String ROLE_TECH = "ROLE_TECH";
  public static final String ROLE_CS_ROOT = IContentService.ROLE_ROOT;

  @Autowired
  protected UserConfig userDetailsConfig;

  @Autowired
  protected IJwtConfig jwtConfig;

  /**
   * As of Spring Security 5.0, the way the passwords are encoded must
   * be specified. When logging, the input password will be encoded
   * and compared with the stored encoded password. To determine which
   * encoding function was used to encode the password, the stored
   * encoded passwords are prefixed with the id of the encoding function.
   * <p>
   * In order to avoid reformatting existing passwords in databases one can
   * set the default <code>PasswordEncoder</code> to use for stored
   * passwords that are not prefixed. This is the role of the following
   * function.
   * <p>More information can be found in the
   * <a href=https://docs.spring.io/spring-security/site/docs/current/reference/html/core-services.html#core-services-password-encoding />
   * Spring documentation</a>
   */
  @Bean
  public PasswordEncoder passwordEncoder() {
    PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    ((DelegatingPasswordEncoder) passwordEncoder).setDefaultPasswordEncoderForMatches(
        NoOpPasswordEncoder.getInstance());
    return passwordEncoder;
  }

  /**
   * Returns the default {@link AuthenticationEntryPoint} to use
   * for the fallback basic HTTP authentication.
   *
   * @return The default {@link AuthenticationEntryPoint} for the
   *         fallback HTTP basic authentication.
   */
  @Bean(name = BASIC_AUTH_BEAN_NAME)
  public AuthenticationEntryPoint basicAuthenticationEntryPoint() {
    return new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED);
  }

  /**
   * Configures the authentication of the whole application.
   *
   * <p>This binds the defined user service to the authentication and sets the source
   * for JWT tokens.
   *
   *  @param auth Spring builder to manage authentication
   * @throws Exception in case of error
   */
  @Autowired
  public void configureGlobal(final AuthenticationManagerBuilder auth) throws Exception {
    auth
        .eraseCredentials(false)
        // Add an LDAP authentication provider instead of this to support LDAP
        .userDetailsService(this.userDetailsConfig.userDetailsService()).and()
        // Required to allow JWT
        .authenticationProvider(jwtConfig.jwtAuthenticationProvider());
  }

  /**
   * [Bean] Comparator for user roles.
   *
   * <p>Defines the comparator used by:
   * <ul>
   *   <li>com.quartetfs.biz.pivot.security.impl.ContextValueManager#setAuthorityComparator(
   *   IAuthorityComparator)</li>
   *   <li>{@link IJwtService}</li>
   * </ul>
   * @return a comparator that indicates which authority/role prevails over another. <b>NOTICE -
   *         an authority coming AFTER another one prevails over this "previous" authority.</b>
   *         This authority ordering definition is essential to resolve possible ambiguity when,
   *         for a given user, a context value has been defined in more than one authority
   *         applicable to that user. In such case, it is what has been set for the "prevailing"
   *         authority that will be effectively retained for that context value for that user.
   */
  @Bean
  public IAuthorityComparator authorityComparator() {
    final CustomComparator<String> comp = new CustomComparator<>();
    comp.setFirstObjects(Arrays.asList(ROLE_USER));
    comp.setLastObjects(Arrays.asList(ROLE_ADMIN));
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

    /**
     * {@code true} to enable the logout URL.
     */
    protected final boolean logout;
    /** The name of the cookie to clear. */
    protected final String cookieName;

    @Autowired
    protected Environment env;

    @Autowired
    protected ApplicationContext context;

    /**
     * This constructor does not enable the logout URL.
     */
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
     * <p>This configures a new firewall accepting `%` in URLs, as none of the core services encode
     * information in URL. This prevents from double-decoding exploits.<br>
     * The firewall is also configured to accept `\` - backslash - as none of ActiveViam APIs offer
     * to manipulate files from URL parameters.<br>
     * Yet, nor `/` and `.` - slash and point - are accepted, as it may trick the REGEXP matchers
     * used for security. Support for those two characters can be added at your own risk, by
     * extending this method. As far as ActiveViam APIs are concerned, `/` and `.` in URL parameters
     * do not represent any risk. `;` - semi-colon - is also not supported, for various APIs end up
     * target an actual database, and because this character is less likely to be used.
     * </p>
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
      final Filter jwtFilter = context.getBean(IJwtConfig.class).jwtFilter();

      http
          // As of Spring Security 4.0, CSRF protection is enabled by default.
          .csrf().disable()
          .cors().and()
          // To allow authentication with JWT (Required for ActiveUI)
          .addFilterAfter(jwtFilter, SecurityContextPersistenceFilter.class);

      if (logout) {
        // Configure logout URL
        http.logout()
            .permitAll()
            .deleteCookies(cookieName)
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
     * @see #configure(HttpSecurity)
     */
    protected abstract void doConfigure(HttpSecurity http) throws Exception;
  }
  /**
   * Configuration for Version service to allow anyone to access this service
   *
   * @author Quartet FS
   * @see HttpStatusEntryPoint
   */
  public abstract static class AVersionSecurityConfigurer extends WebSecurityConfigurerAdapter {

    /** The autowired Spring context */
    @Autowired protected ApplicationContext context;

    @Override
    protected void configure(HttpSecurity http) throws Exception {

      http.antMatcher(VersionServicesConfig.REST_API_URL_PREFIX + "/**")
          // As of Spring Security 4.0, CSRF protection is enabled by default.
          .csrf().disable()
          .cors().and()
          .authorizeRequests()
          .antMatchers("/**")
          .permitAll();
    }
  }

  /**
   * Configuration for ActiveUI.
   *
   * @author Quartet FS
   * @see HttpStatusEntryPoint
   */
  public abstract static class AActiveUISecurityConfigurer extends AWebSecurityConfigurer {

    @Override
    protected void doConfigure(HttpSecurity http) throws Exception {
      // Permit all on ActiveUI resources and the root (/) that redirects to ActiveUI index.html.
      final String pattern = "^(.{0}|\\/|\\/" + "ui" + "(\\/.*)?)$";
      http
          // Only theses URLs must be handled by this HttpSecurity
          .regexMatcher(pattern)
          .authorizeRequests()
          // The order of the matchers matters
          .regexMatchers(HttpMethod.OPTIONS, pattern)
          .permitAll()
          .regexMatchers(HttpMethod.GET, pattern)
          .permitAll();

      // Authorizing pages to be embedded in iframes to have ActiveUI in ActiveMonitor UI
      http.headers().frameOptions().disable();
    }
  }
}
