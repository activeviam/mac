/*
 * (C) ActiveViam 2012
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Simple log record formatter that logs the user authenticated with the current thread, and the
 * name of that thread.
 *
 * <p>Retrieving the authenticated user is done (and only works with) the Spring Security
 * framework.
 *
 * <p>The formatter can be configured with those system properties:
 *
 * <ul>
 *   <li>-DlogThread=true
 *   <li>-DlogUser=true
 *   <li>-DlogLoggerName=true
 * </ul>
 *
 * @author Quartet FS
 */
public class QFSFormatter extends Formatter {

	/**
	 * System property to activate logging the current user.
	 */
	public static final String LOG_USER_PROPERTY = "logUser";

	/**
	 * System property to activate logging the current thread.
	 */
	public static final String LOG_THREAD_PROPERTY = "logThread";

	/**
	 * System property to activate logging the name of the logger.
	 */
	public static final String LOG_LOGGER_NAME_PROPERTY = "logLoggerName";

	/**
	 * Current time.
	 */
	protected final Calendar time;

	/**
	 * Flag to log the current thread.
	 */
	protected final boolean logThread;

	/**
	 * Flag to log the current user.
	 */
	protected final boolean logUser;

	/**
	 * Flag to log the name of the logger.
	 */
	protected final boolean logLoggerName;

	/**
	 * Constructor.
	 */
	public QFSFormatter() {
		this.time = Calendar.getInstance();

		this.logThread = Boolean.parseBoolean(System.getProperty(LOG_THREAD_PROPERTY, "true"));
		this.logUser = Boolean.parseBoolean(System.getProperty(LOG_USER_PROPERTY, "true"));
		this.logLoggerName =
				Boolean.parseBoolean(System.getProperty(LOG_LOGGER_NAME_PROPERTY, "false"));
	}

	@Override
	public String format(LogRecord record) {
		time.setTimeInMillis(record.getMillis());
		String source;
		if (record.getSourceClassName() != null) {
			source = record.getSourceClassName();
			if (record.getSourceMethodName() != null) {
				source += " " + record.getSourceMethodName();
			}
		} else {
			source = record.getLoggerName();
		}
		String message = formatMessage(record);

		String throwable = null;
		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			try (final PrintWriter pw = new PrintWriter(sw)) {
				pw.println();
				record.getThrown().printStackTrace(pw);
			}
			throwable = sw.toString();
		}

		StringBuilder builder = new StringBuilder(record.getLevel().getLocalizedName()).append(":");
		if (logLoggerName) {
			builder.append(" (").append(record.getLoggerName()).append(")");
		}
		if (logThread) {
			builder.append(" thread=").append(getCurrentThread());
		}
		if (logUser) {
			String user = getCurrentUser();
			if (user != null) {
				builder.append(" user=").append(user);
			}
		}
		builder.append(" ");

		builder.append(time.getTime()).append(' ').append(source);
		builder.append('\n').append(message).append('\n');

		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			try (PrintWriter pw = new PrintWriter(sw)) {
				pw.println();
				record.getThrown().printStackTrace(pw);
			}
			throwable = sw.toString();
			builder.append('\n').append(throwable);
		}

		return builder.toString();
	}

	/**
	 * Returns the name of the current Thread.
	 *
	 * @return the name of the thread on which this method was invoked.
	 */
	public String getCurrentThread() {
		return Thread.currentThread().getName();
	}

	/**
	 * Retrieves the current user authenticated with the current thread.
	 *
	 * @return current authenticated user, or null if no user is authenticated with the current
	 * 				thread.
	 */
	public String getCurrentUser() {
		Authentication currentAuthentication = SecurityContextHolder.getContext().getAuthentication();
		if (currentAuthentication != null) {
			return currentAuthentication.getName();
		}
		return null;
	}
}
