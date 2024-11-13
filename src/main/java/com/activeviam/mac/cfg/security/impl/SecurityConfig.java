package com.activeviam.mac.cfg.security.impl;

import static com.activeviam.web.core.api.IUrlBuilder.url;

import com.activeviam.activepivot.server.impl.private_.spring.ContextValueFilter;
import com.activeviam.activepivot.server.spring.private_.pivot.security.impl.UserDetailsServiceWrapper;
import com.activeviam.tech.contentserver.storage.api.IContentService;
import com.activeviam.tech.core.api.security.IUserDetailsService;
import com.activeviam.web.spring.api.config.ICorsConfig;
import com.activeviam.web.spring.api.config.IJwtConfig;
import com.activeviam.web.spring.api.jwt.JwtAuthenticationProvider;
import com.activeviam.web.spring.internal.config.JwtRestServiceConfig;
import com.activeviam.web.spring.internal.security.NoRedirectLogoutSuccessHandler;
import com.activeviam.web.spring.private_.jwt.JwtFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;
import org.springframework.security.web.context.SecurityContextPersistenceFilter;
import org.springframework.security.web.firewall.StrictHttpFirewall;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@Configuration
public class SecurityConfig {

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

  /** The address the UI is exposed to. */
  public static final String ACTIVEUI_ADDRESS = "activeui.address";

  private static final String REST_API_URL_PREFIX = "/versions/rest";
  /** Name of the Cookies of the MAC application. */
  public static final String COOKIE_NAME = "MEMORY_ANALYSIS_CUBE";

  private static final String PING_SUFFIX = "/ping";

  /** The User Configuration. */
  @Autowired protected UserConfig userDetailsConfig;

  /** The JWT Configuration. */
  @Autowired protected IJwtConfig jwtConfig;
  /** The name of the Environment to use. */
  @Autowired protected Environment env;

  /** {@code true} to enable the logout URL. */
  protected final WebConfiguration webConfiguration;

  public SecurityConfig(final String cookieName) {
    this.webConfiguration = new WebConfiguration(cookieName);
  }

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
  @Bean
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
  public void configureGlobal(
      final AuthenticationManagerBuilder auth,
      final JwtAuthenticationProvider jwtAuthenticationProvider) {
    auth
        // Required to allow JWT
        .authenticationProvider(jwtAuthenticationProvider);
  }

  /**
   * [Bean] Spring standard way of configuring CORS.
   *
   * <p>This simply forwards the configuration of {@link ICorsConfig} to Spring security system.
   *
   * @return the configuration for the application.
   */
  @Bean
  public CorsConfigurationSource corsConfigurationSource(final ICorsConfig corsConfig) {
    final CorsConfiguration configuration = new CorsConfiguration();
    configuration.setAllowedOrigins(corsConfig.getAllowedOrigins());
    configuration.setAllowedHeaders(corsConfig.getAllowedHeaders());
    configuration.setExposedHeaders(corsConfig.getExposedHeaders());
    configuration.setAllowedMethods(corsConfig.getAllowedMethods());
    configuration.setAllowCredentials(true);

    final UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", configuration);

    return source;
  }

  /**
   * Returns the spring security bean user details service wrapper.
   *
   * @return the {@link IUserDetailsService} used as spring security bean user details service
   *     wrapper.
   */
  @Bean
  public IUserDetailsService qfsUserDetailsService() {
    return new UserDetailsServiceWrapper(this.userDetailsConfig.userDetailsService());
  }

  /**
   * Returns a bean initializing the Cookies name in the Servlet Spring context.
   *
   * @return the bean initializing the Cookies name in the Servlet Spring context
   */
  @Bean
  public ServletContextInitializer servletContextInitializer() {
    return servletContext -> servletContext.getSessionCookieConfig().setName(COOKIE_NAME);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This configures a new firewall accepting `%` in URLs, as none of the core services encode
   * information in URL. This prevents from double-decoding exploits.<br>
   * The firewall is also configured to accept `\` - backslash - as none of ActiveViam APIs offer to
   * manipulate files from URL parameters.<br>
   * Yet, nor `/` and `.` - slash and point - are accepted, as it may trick the REGEXP matchers used
   * for security. Support for those two characters can be added at your own risk, by extending this
   * method. As far as ActiveViam APIs are concerned, `/` and `.` in URL parameters do not represent
   * any risk. `;` - semi-colon - is also not supported, for various APIs end up target an actual
   * database, and because this character is less likely to be used.
   */
  @Bean
  public StrictHttpFirewall configureFirewall() {
    final StrictHttpFirewall firewall = new StrictHttpFirewall();
    firewall.setAllowUrlEncodedPercent(true);
    firewall.setAllowBackSlash(true);

    firewall.setAllowUrlEncodedSlash(false);
    firewall.setAllowUrlEncodedPeriod(false);
    firewall.setAllowSemicolon(false);
    return firewall;
  }

  @Bean
  @Order(1)
  public SecurityFilterChain activeUiSecurity(final HttpSecurity http, final JwtFilter jwtFilter)
      throws Exception {
    configureWebSecurity(http, jwtFilter, this.webConfiguration);
    final String activeUiUrl = env.getRequiredProperty(ACTIVEUI_ADDRESS);
    // Only these URLs must be handled by this filter chain
    http.securityMatcher(url(activeUiUrl))
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(HttpMethod.OPTIONS)
                    .permitAll()
                    .requestMatchers(HttpMethod.GET)
                    .permitAll());
    // this allows pre-flight cross-origin requests
    http.cors(Customizer.withDefaults());
    // Authorizing pages to be embedded in iframes to have ActiveUI in ActiveMonitor UI
    http.headers(
        customizer -> customizer.frameOptions(HeadersConfigurer.FrameOptionsConfig::disable));
    return http.build();
  }

  @Bean
  @Order(2)
  public SecurityFilterChain jwtSecurity(
      final HttpSecurity http,
      final ApplicationContext applicationContext,
      final AuthenticationEntryPoint authenticationEntryPoint)
      throws Exception {
    http.securityMatcher(JwtRestServiceConfig.REST_API_URL_PREFIX + "/**")
        // As of Spring Security 4.0, CSRF protection is enabled by default.
        .csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        // Configure CORS
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(HttpMethod.OPTIONS, "/**")
                    .permitAll()
                    .requestMatchers("/**")
                    .hasAnyAuthority(ROLE_USER))
        .httpBasic(customizer -> customizer.authenticationEntryPoint(authenticationEntryPoint));
    return http.build();
  }

  @Bean
  @Order(3)
  public SecurityFilterChain versionSecurity(final HttpSecurity http) throws Exception {
    http.securityMatcher(REST_API_URL_PREFIX + "/**")
        // As of Spring Security 4.0, CSRF protection is enabled by default.
        .csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        .authorizeHttpRequests(auth -> auth.requestMatchers("/**").permitAll());
    return http.build();
  }

  @Bean
  @Order(4)
  public SecurityFilterChain activePivotSecurity(
      final HttpSecurity http,
      final JwtFilter jwtFilter,
      final ContextValueFilter contextValueFilter)
      throws Exception {
    configureWebSecurity(http, jwtFilter, this.webConfiguration);
    http
        // The ping service is temporarily authenticated (see PIVOT-3149)
        .securityMatcher(url(REST_API_URL_PREFIX, PING_SUFFIX))
        .authorizeHttpRequests(
            auth -> auth.requestMatchers(HttpMethod.GET).hasAnyAuthority(ROLE_USER, ROLE_TECH))
        // Rest services
        .securityMatcher(REST_API_URL_PREFIX + "/**")
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(HttpMethod.OPTIONS)
                    .permitAll()
                    .anyRequest()
                    .hasAnyAuthority(ROLE_USER))
        // One has to be a user for all the other URLs
        .securityMatcher("/**")
        .authorizeHttpRequests(auth -> auth.anyRequest().hasAnyAuthority(ROLE_USER))
        .httpBasic(Customizer.withDefaults())
        // SwitchUserFilter is the last filter in the chain. See FilterComparator class.
        .addFilterAfter(contextValueFilter, SwitchUserFilter.class);
    return http.build();
  }

  public void configureWebSecurity(
      HttpSecurity http, JwtFilter jwtFilter, WebConfiguration webConfig) throws Exception {
    http
        // As of Spring Security 4.0, CSRF protection is enabled by default.
        .csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        // To allow authentication with JWT (Required for ActiveUI)
        .addFilterAfter(jwtFilter, SecurityContextPersistenceFilter.class);

    if (webConfig.logout) {
      // Configure logout URL
      http.logout(
          auth ->
              auth.permitAll()
                  .deleteCookies(webConfig.cookieName)
                  .invalidateHttpSession(true)
                  .logoutSuccessHandler(new NoRedirectLogoutSuccessHandler()));
    }

    if (webConfig.useAnonymous) {
      // Handle anonymous users. The granted authority ROLE_USER
      // will be assigned to the anonymous request
      http.anonymous(customizer -> customizer.principal("guest").authorities(ROLE_USER));
    }
  }
}
