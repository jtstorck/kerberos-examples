package hadoop.ugitest;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class UgiTestMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(UgiTestMain.class);

    private static final String JAVA_HOME = "JAVA_HOME";

    private static final String HEADER = System.lineSeparator() + "A tool for testing the usage of hadoop-client UserGroupInformation with a kerberized HDFS cluster." +
            System.lineSeparator() + System.lineSeparator();
    private static final String FOOTER = System.lineSeparator() + "Java home: " + System.getenv(JAVA_HOME) + System.lineSeparator();

    private static final Option OPTION_KRB_CONF_FILE = Option.builder("k")
            .longOpt("krb-conf")
            .desc("Kerberos configuration path")
            .hasArg()
            .argName("file")
            .required()
            .build();
    private static final Option OPTION_HADOOP_MIN_RELOGIN_PERIOD_SECONDS = Option.builder("m")
            .longOpt("hadoop-relogin-min-delay")
            .desc("Hadoop minimum relogin period (in seconds), only supported with hadoop-client 2.8+")
            .hasArgs()
            .numberOfArgs(1)
            .argName("seconds")
            .build();
    private static final Option OPTION_TASK_CONFIG_JSON_FILE = Option.builder("c")
            .longOpt("task-config")
            .desc("Task configuration file (JSON)")
            .hasArgs()
            .numberOfArgs(1)
            .argName("file")
            .type(File.class)
            .required()
            .build();
    private static final Option OPTION_HADOOP_RESOURCE = Option.builder("r")
            .longOpt("hadoop-resource")
            .desc("site.xml file used by Hadoop (repeatable argument)")
            .argName("file")
            .numberOfArgs(1)
            .build();
    private static final Option OPTION_MIGRATOR_HELP = Option.builder("h")
            .longOpt("help")
            .desc("display help/usage info")
            .build();

    private static ScheduledExecutorService scheduledExecutorService;
    private static volatile boolean running = true;
    private static Gson gson = new GsonBuilder().create();

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
                final String hadoopMinReloginSeconds = commandLine.getOptionValue(OPTION_HADOOP_MIN_RELOGIN_PERIOD_SECONDS.getOpt(), "600");
                configuration.set("hadoop.kerberos.min.seconds.before.relogin", hadoopMinReloginSeconds);
                // fix fs impl properties, since maven-assembly-plugin causes some of the META-INF/services files to get overwritten
                // when files of the same name exist in multiple modules
                configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                String[] hadoopResourceOptionValues = MoreObjects.firstNonNull(commandLine.getOptionValues(OPTION_HADOOP_RESOURCE.getOpt()), new String[]{});
                if (hadoopResourceOptionValues.length == 0) {
                    LOGGER.warn("No Hadoop resources were specified");
                }
                Lists.newArrayList(hadoopResourceOptionValues).forEach(s -> configuration.addResource(new Path(s)));
                UserGroupInformation.setConfiguration(configuration);

                LOGGER.info("Using kerberos config [{}], hadoop config [{}], hadoop kerberos min seconds before relogin [{}]",
                        kerberosConfig, configuration, hadoopMinReloginSeconds);

                // set up kerberos credentials and tasks
                final File taskConfigFile = (File) commandLine.getParsedOptionValue(OPTION_TASK_CONFIG_JSON_FILE.getOpt());
                Preconditions.checkArgument(taskConfigFile.exists(), "Task configuration file [%s] not found", taskConfigFile);
                Type taskConfigCollectionType = new TypeToken<LinkedList<TaskConfig>>() {
                }.getType();
                final Collection<TaskConfig> taskConfigs = gson.fromJson(new JsonReader(new FileReader(taskConfigFile)), taskConfigCollectionType);
                Preconditions.checkState(taskConfigs != null, "No tasks were parsed from [%s]", taskConfigFile);
                LOGGER.info("Tasks to be created: {}", taskConfigs);
                scheduledExecutorService = Executors.newScheduledThreadPool(taskConfigs.size());
                for (TaskConfig taskConfig : taskConfigs) {
                    final String keytab = taskConfig.getKeytabPath();
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(keytab), "No keytab was provided");
                    final String principal = taskConfig.getPrincipal();
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(principal), "No principal was provided");
                    LOGGER.debug("Attempting new login for principal [{}] from keytab [{}]", principal, keytab);
                    final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                    final long firstLoginTimstampSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                    LOGGER.debug("Acquired UGI for principal [{}] from keytab [{}], UGI [{}]", principal, keytab, ugi);
                    scheduledExecutorService.scheduleAtFixedRate(new UgiRunnable(ugi, configuration, taskConfig, firstLoginTimstampSeconds),
                            taskConfig.getInitialTaskDelay(), taskConfig.getTaskPeriod(), TimeUnit.SECONDS);
                    LOGGER.debug("Scheduled task for principal [{}] from keytab [{}], UGI [{}], initial task execution delay [{} second(s)], task execution period [{} second(s)], relogin period [{} second(s)]",
                            principal, keytab, ugi, taskConfig.getInitialTaskDelay(), taskConfig.getTaskPeriod(), taskConfig.getReloginPeriod());
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
        options.addOption(OPTION_HADOOP_MIN_RELOGIN_PERIOD_SECONDS);
        options.addOption(OPTION_HADOOP_RESOURCE);
        options.addOption(OPTION_TASK_CONFIG_JSON_FILE);
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
        private final TaskConfig taskConfig;
        private long lastLoginTimestampSeconds;

        UgiRunnable(UserGroupInformation ugi, Configuration configuration, TaskConfig taskConfig, long firstLoginTimestampSeconds) {
            lastLoginTimestampSeconds = firstLoginTimestampSeconds;
            this.ugi = ugi;
            this.configuration = configuration;
            this.taskConfig = taskConfig;
            LOGGER = LoggerFactory.getLogger(UgiRunnable.class.getCanonicalName() + "." + ugi.getUserName());
        }

        @Override
        public void run() {
            try {
                if ((TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - lastLoginTimestampSeconds) > taskConfig.getReloginPeriod()) {
                    LOGGER.debug("Attempting relogin for [{}]", ugi);
                    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                        ugi.reloginFromKeytab();
                        return null;
                    });
                    lastLoginTimestampSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                }
                Path hdfsDirPath = new Path(taskConfig.getDestinationPath());
                Path hdfsDestinationPath = new Path(hdfsDirPath, UUID.randomUUID().toString());
                LOGGER.trace("Attempting to create file [{}] by [{}]", hdfsDestinationPath, ugi);
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    FileSystem fileSystem = FileSystem.get(configuration);
                    try (FSDataOutputStream outputStream = fileSystem.create(hdfsDestinationPath, true)) {
                        outputStream.writeChars(String.format("Written at %s", new Date()));
                        outputStream.flush();
                        LOGGER.info("Created file [{}] by [{}]", hdfsDestinationPath, ugi);
                    }
                    return null;
                });
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
