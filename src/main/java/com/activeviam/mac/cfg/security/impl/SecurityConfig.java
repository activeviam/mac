package com.activeviam.mac.cfg.security.impl;

import com.activeviam.tech.contentserver.storage.api.IContentService;
import com.activeviam.web.spring.api.config.IJwtConfig;
import com.activeviam.web.spring.api.jwt.JwtAuthenticationProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.User.UserBuilder;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.firewall.StrictHttpFirewall;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

  /** Name of the User Role. */
  public static final String ROLE_USER = "ROLE_USER";

  /** Name of the Admin Role. */
  public static final String ROLE_ADMIN = "ROLE_ADMIN";

  /** Name of the Cookies of the MAC application. */
  public static final String COOKIE_NAME = "MEMORY_ANALYSIS_CUBE";

  /**
   * [Bean] Create the users that can access the application.
   *
   * @return {@link UserDetailsService user data}
   */
  @Bean
  public UserDetailsService userDetailsService(final PasswordEncoder passwordEncoder) {
    final UserBuilder builder = User.builder().passwordEncoder(passwordEncoder::encode);
    final InMemoryUserDetailsManager service = new InMemoryUserDetailsManager();
    service.createUser(
        builder
            .username("admin")
            .password("admin")
            .authorities(ROLE_USER, ROLE_ADMIN, IContentService.ROLE_ROOT)
            .build());
    return service;
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
   * @param inMemoryAuthenticationProvider the in-memory authentication provider
   * @param jwtAuthenticationProvider is a provider which can perform authentication from the
   *     jwtService's tokens. Implementation from the {@link IJwtConfig} .
   * @return the authentication manager
   */
  @Bean
  public AuthenticationManager authenticationManager(
      final JwtAuthenticationProvider jwtAuthenticationProvider,
      final AuthenticationProvider inMemoryAuthenticationProvider) {
    final ProviderManager providerManager =
        new ProviderManager(inMemoryAuthenticationProvider, jwtAuthenticationProvider);
    providerManager.setEraseCredentialsAfterAuthentication(false);

    return providerManager;
  }

  @Bean
  public AuthenticationProvider inMemoryAuthenticationProvider(
      final UserDetailsService userDetailsService, final PasswordEncoder passwordEncoder) {
    final var authenticationProvider = new DaoAuthenticationProvider();
    authenticationProvider.setPasswordEncoder(passwordEncoder);
    authenticationProvider.setUserDetailsService(userDetailsService);

    return authenticationProvider;
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
}
