package kite.log;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


/**
 * A simple wrapper around java.util.logging.Logger.
 * It mostly adds support for easier method calling and being able to use varargs.
 * I find it easier to read the logging.properties file from classpath here too.
 *
 * Also, look for the jul-to-slf4j bridge, it might help you if you already use slf4j or other logging systems.
 */
public class Log {
    static {
        // the properties must be set before getting a Logger
        var classLoader = Log.class.getClassLoader();
        try (var is = classLoader.getResourceAsStream("logging.properties")) {
            LogManager.getLogManager().readConfiguration(is);
        } catch (IOException e) {
            IO.println("Could not load logging.properties from classpath. Using defaults settings for logging.");
        }
    }

    public static Log getLog(Class<?> clazz) {
        return new Log(clazz);
    }

    private final Logger logger;

    private Log(Class<?> clazz) {
        logger = Logger.getLogger(clazz.getName());
    }

    public void fine(String message) {
        log(Level.FINE, message);
    }

    public void fine(String message, Object... args) {
        log(Level.FINE, message, args);
    }

    public void fine(String message, Throwable thrown) {
        log(Level.FINE, message, thrown);
    }

    public void info(String message) {
        log(Level.INFO, message);
    }

    public void info(String message, Object... args) {
        log(Level.INFO, message, args);
    }

    public void info(String message, Throwable thrown) {
        log(Level.INFO, message, thrown);
    }

    public void warning(String message) {
        log(Level.WARNING, message);
    }

    public void warning(String message, Object... args) {
        log(Level.WARNING, message, args);
    }

    public void warning(String message, Throwable thrown) {
        log(Level.WARNING, message, thrown);
    }

    public void severe(String message) {
        log(Level.SEVERE, message);
    }

    public void severe(String message, Object... args) {
        log(Level.SEVERE, message, args);
    }

    public void severe(String message, Throwable thrown) {
        log(Level.SEVERE, message, thrown);
    }

    private void log(Level level, String message) {
        if (!logger.isLoggable(level)) {
            return;
        }

        var lr = new KiteLogRecord(level, message);
        logger.log(lr);
    }

    private void log(Level level, String message, Object... args) {
        if (!logger.isLoggable(level)) {
            return;
        }

        var lr = new KiteLogRecord(level, message);
        lr.setParameters(args);
        logger.log(lr);
    }

    private void log(Level level, String message, Throwable thrown) {
        if (!logger.isLoggable(level)) {
            return;
        }

        var lr = new KiteLogRecord(level, message);
        lr.setThrown(thrown);
        logger.log(lr);
    }

    /**
     * We need to override and reuse code from the JDK in order to get
     * correctly the class and method names.
     */
    static final class KiteLogRecord extends LogRecord {

        private boolean findCaller;
        private String sourceClassName;
        private String sourceMethodName;

        public KiteLogRecord(Level level, String msg) {
            super(level, msg);
            this.findCaller = true;
        }

        @Override
        public String getSourceClassName() {
            if (findCaller) {
                findCaller();
            }

            return sourceClassName;
        }

        @Override
        public String getSourceMethodName() {
            if (findCaller) {
                findCaller();
            }

            return sourceMethodName;
        }

        private void findCaller() {
            findCaller = false;
            // Skip all frames until we have found the first logger frame.
            var finder = new StackFrameFinder();
            var frame = finder.findStackFrame();

            frame.ifPresent(f -> {
                sourceClassName = f.getClassName();
                sourceMethodName = f.getMethodName();
            });

            // if we haven't found a suitable frame, this is
            // OK as we are only committed to making a "best effort" here.
        }
    }


    /*
     * StackFrameFinder is a stateful predicate.
     */
    static final class StackFrameFinder implements Predicate<StackWalker.StackFrame> {
        private static final StackWalker WALKER =
                StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);


        /**
         * Returns StackFrame of the caller's frame.
         * @return StackFrame of the caller's frame.
         */
        Optional<StackWalker.StackFrame> findStackFrame() {
            return WALKER.walk((s) -> s.filter(this).findFirst());
        }

        private boolean lookingForLogger = true;

        /**
         * Returns true if we have found the caller's frame, false if the frame
         * must be skipped.
         *
         * @param t The frame info.
         * @return true if we have found the caller's frame, false if the frame
         * must be skipped.
         */
        @Override
        public boolean test(StackWalker.StackFrame t) {
            // false if the frame needs to be skipped
            // true if we found the caller's frame

            final String cname = t.getClassName();
            // We should skip all frames until we have found the logger,
            // because these frames could be frames introduced by e.g. custom
            // sub classes of Handler.
            if (lookingForLogger) {
                // the log record could be created for a platform logger
                lookingForLogger = !isLoggerImplFrame(cname);
                return false;
            }

            // Continue walking until we've found the relevant calling frame.

            // skip Kite's Log class
            if (t.getDeclaringClass().equals(Log.class)) {
                // since we are calling private methods in kite.log.Log
                // we get more than 1 StackFrame, we need to skip those frames too
                // so keep walking
                return false;
            }

            // Skips logging/logger infrastructure.
            return !isFilteredFrame(t);
        }

        private boolean isLoggerImplFrame(String cname) {
            return (cname.equals("java.util.logging.Logger") ||
                    cname.startsWith("sun.util.logging.PlatformLogger"));
        }

        static boolean isFilteredFrame(StackWalker.StackFrame st) {
            // skip logging/logger infrastructure
            if (System.Logger.class.isAssignableFrom(st.getDeclaringClass())) {
                return true;
            }

            // fast escape path: all the prefixes below start with 's' or 'j' and
            // have more than 12 characters.
            final String cname = st.getClassName();
            char c = cname.length() < 12 ? 0 : cname.charAt(0);
            if (c == 's') {
                // skip internal machinery classes
                if (cname.startsWith("sun.util.logging."))   return true;
                if (cname.startsWith("sun.rmi.runtime.Log")) return true;
            } else if (c == 'j') {
                // Message delayed at Bootstrap: no need to go further up.
                if (cname.startsWith("jdk.internal.logger.BootstrapLogger$LogEvent")) return false;
                // skip public machinery classes
                if (cname.startsWith("jdk.internal.logger."))          return true;
                if (cname.startsWith("java.util.logging."))            return true;
                if (cname.startsWith("java.lang.invoke.MethodHandle")) return true;

                return cname.startsWith("java.security.AccessController");
            }

            return false;
        }
    }

}
