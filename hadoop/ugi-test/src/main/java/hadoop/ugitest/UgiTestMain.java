package hadoop.ugitest;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UgiTestMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(UgiTestMain.class);

    private static final String JAVA_HOME = "JAVA_HOME";

    private static final String HEADER = System.lineSeparator() + "A tool for testing logins and relogins to a kerberized HDFS cluster." +
            System.lineSeparator() + System.lineSeparator();
    private static final String FOOTER = System.lineSeparator() + "Java home: " + System.getenv(JAVA_HOME) + System.lineSeparator();

    private static final Option OPTION_KRB_CONF_FILE = Option.builder("k")
            .longOpt("krb-conf")
            .desc("Kerberos configuration path")
            .hasArg()
            .argName("filename")
            .numberOfArgs(1)
            .required()
            .build();
    private static final Option OPTION_KRB_CREDENTIALS = Option.builder("c")
            .longOpt("krb-credential")
            .desc("Kerberos keytab, principal separated by ','")
            .hasArgs()
            .argName("keytab,principal")
            .valueSeparator(',')
            .required()
            .build();
    private static final Option OPTION_KRB_RELOGIN_PERIOD = Option.builder("p")
            .longOpt("relogin-period")
            .desc("Kerberos relogin period in seconds")
            .hasArgs()
            .argName("seconds")
            .valueSeparator(',')
            .build();
    private static final Option OPTION_HADOOP_RESOURCES = Option.builder("r")
            .longOpt("hadoop-resources")
            .desc("*-site.xml files used by Hadoop, separated by ','")
            .argName("paths")
            .hasArgs()
            .valueSeparator(',')
            .required()
            .build();
    private static final Option OPTION_HDFS_DIR = Option.builder("d")
            .longOpt("dir")
            .desc("HDFS directory for which status will be retrieved")
            .argName("path")
            .hasArg()
            .required()
            .build();
    private static final Option OPTION_MIGRATOR_HELP = Option.builder("h")
            .longOpt("help")
            .desc("display help/usage info")
            .build();

    private static ScheduledExecutorService scheduledExecutorService;
    private static volatile boolean running = true;

    public static void main(String[] args) throws IOException, InterruptedException {
        PrintStream output = System.out;
        System.setOut(System.err);

        final Options options = createOptions();
        final CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(options, args);
            if (commandLine.hasOption(OPTION_MIGRATOR_HELP.getLongOpt())) {
                printUsage(null, options, output);
            } else {
                // set up krb config
                final String kerberosConfig = commandLine.getOptionValue(OPTION_KRB_CONF_FILE.getOpt());
                Preconditions.checkArgument(!Strings.isNullOrEmpty(kerberosConfig), "No kerberos configuration was provided");
                System.setProperty("java.security.krb5.conf", kerberosConfig);

                // set up hadoop resources configuration
                Configuration configuration = new Configuration();
                configuration.set("hadoop.kerberos.min.seconds.before.relogin", "1");
                // fix fs impl properties, since maven-assembly-plugin causes some of the META-INF/services files to get overwritten
                // when files of the same name exist in multiple modules
                configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                String[] hadoopResourcesOptionValues = commandLine.getOptionValues(OPTION_HADOOP_RESOURCES.getOpt());
                Lists.newArrayList(hadoopResourcesOptionValues).forEach(s -> configuration.addResource(new Path(s)));

                UserGroupInformation.setConfiguration(configuration);

                LOGGER.info("Test using kerberos config [{}], hadoop config [{}]", kerberosConfig, configuration);

                // set up kerberos credentials and tasks
                final int reloginAttemptPeriod = commandLine.hasOption(OPTION_KRB_RELOGIN_PERIOD.getOpt()) ? Integer.parseInt(commandLine.getOptionValue(OPTION_KRB_RELOGIN_PERIOD.getOpt())) :
                        60;
                final String hdfsDir = commandLine.getOptionValue(OPTION_HDFS_DIR.getOpt());
                Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsDir), "No HDFS path was provided");
                final String[] krbCredentialOptionValues = commandLine.getOptionValues(OPTION_KRB_CREDENTIALS.getOpt());
                if (krbCredentialOptionValues.length % 2 != 0) {
                    throw new IllegalArgumentException(String.format("command-line parameter %s requires one or more keytab and principal pairs separated by a ',', unable to parse properly",
                            OPTION_KRB_CREDENTIALS.getLongOpt()));
                }
                scheduledExecutorService = Executors.newScheduledThreadPool(krbCredentialOptionValues.length / 2);
                for (int i = 0; i < krbCredentialOptionValues.length; i = i + 2) {
                    final String keytab = krbCredentialOptionValues[i];
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(keytab), "No keytab was provided");
                    final String principal = krbCredentialOptionValues[i + 1];
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(principal), "No principal was provided");
                    LOGGER.info("Attempting new login for principal [{}] from keytab [{}]", principal, keytab);
                    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                    LOGGER.info("UGI acquired for principal [{}] from keytab [{}], UGI [{}]", principal, keytab, ugi);
                    LOGGER.info("Scheduling relogin for principal [{}] from keytab [{}], UGI [{}], relogin period [{} second(s)]", principal, keytab, ugi, reloginAttemptPeriod);
                    scheduledExecutorService.scheduleAtFixedRate(new UgiRunnable(ugi, configuration, hdfsDir), 0, reloginAttemptPeriod, TimeUnit.SECONDS);
                }
            }
            while (!scheduledExecutorService.isTerminated()) {
                if (!running) {
                    LOGGER.info("shutting down executor service threads");
                    scheduledExecutorService.shutdownNow();
                }
                LOGGER.trace("awaiting executor service termination");
                scheduledExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS);
            }
        } catch (ParseException e) {
            printUsage(e.getLocalizedMessage(), options, output);
        }
    }

    private static Options createOptions() {
        final Options options = new Options();
        options.addOption(OPTION_MIGRATOR_HELP);
        options.addOption(OPTION_KRB_CONF_FILE);
        options.addOption(OPTION_KRB_RELOGIN_PERIOD);
        options.addOption(OPTION_HADOOP_RESOURCES);
        options.addOption(OPTION_KRB_CREDENTIALS);
        options.addOption(OPTION_HDFS_DIR);
        return options;
    }

    private static void printUsage(String errorMessage, Options options, PrintStream outputStream) {
        Preconditions.checkNotNull(options, "command line options were not specified");
        if (errorMessage != null) {
            outputStream.println(errorMessage + System.lineSeparator());
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.setDescPadding(2);
        helpFormatter.printHelp(UgiTestMain.class.getCanonicalName(), HEADER, options, FOOTER, true);
    }

    private static class UgiRunnable implements Runnable {

        private final Logger LOGGER;
        private final UserGroupInformation ugi;
        private final Configuration configuration;
        private final String hdfsDir;

        UgiRunnable(UserGroupInformation ugi, Configuration configuration, String hdfsDir) {
            this.ugi = ugi;
            this.configuration = configuration;
            LOGGER = LoggerFactory.getLogger(UgiRunnable.class.getCanonicalName() + "." + ugi.getUserName());
            this.hdfsDir = hdfsDir;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Attempting relogin for [{}]", ugi);
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    ugi.checkTGTAndReloginFromKeytab();
                    return null;
                });
                LOGGER.info("Attempting to list status of [{}] with [{}]", hdfsDir, ugi);
                FileSystem fileSystem = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(configuration));
                FileStatus status = ugi.doAs((PrivilegedExceptionAction<FileStatus>) () -> fileSystem.getFileStatus(new Path(hdfsDir)));
                LOGGER.info("Status retrieved for path [{}] by [{}]: {}", hdfsDir, ugi, status);
            } catch (InterruptedException e) {
                running = false;
                Thread.currentThread().interrupt();
                LOGGER.warn("Received interrupt while performing task for [{}]", ugi, e);
            } catch (Exception e) {
                running = false;
                Thread.currentThread().interrupt();
                LOGGER.warn("Unexpected exception while performing task for [{}]", ugi, e);
            }
        }
    }
}
