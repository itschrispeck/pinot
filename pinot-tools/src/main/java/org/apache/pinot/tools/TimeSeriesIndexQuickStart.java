package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeSeriesIndexQuickStart extends QuickStartBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesIndexQuickStart.class);

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "TIMESERIESINDEX"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  @Override
  public List<String> types() {
    return List.of("TIMESERIESINDEX");
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "m3ql");
    configs.put(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey("m3ql"),
        "org.apache.pinot.tsdb.m3ql.M3TimeSeriesPlanner");
    configs.put(PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey("m3ql"),
        SimpleTimeSeriesBuilderFactory.class.getName());
    return configs;
  }

  @Override
  public void execute()
      throws Exception {

    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    QuickstartTableRequest quickstartTableRequest = bootstrapStreamTableDirectory(quickstartTmpDir);
    final QuickstartRunner runner =
        new QuickstartRunner(List.of(quickstartTableRequest), 1, 1, 2, 1, quickstartRunnerDir, getConfigOverrides());

    startKafka();
    _kafkaStarter.createTopic("timeSeriesIndex", KafkaStarterUtils.getTopicCreationProps(2));
    printStatus(Quickstart.Color.CYAN,
        "***** Starting timeSeriesIndex data stream and publishing to Kafka *****");
    publishLineSplitFileToKafka("timeSeriesIndex",
        new File(new File(quickstartTmpDir, "timeSeriesIndex"), "/rawdata/datapoints.json"));

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Quickstart.Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Quickstart.Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Quickstart.Color.YELLOW, "***** Realtime quickstart setup complete *****");
    runSampleQueries(runner);

    printStatus(Quickstart.Color.GREEN,
        String.format("You can always go to http://localhost:%d to play around in the query console",
            QuickstartRunner.DEFAULT_CONTROLLER_PORT));
  }

  protected QuickstartTableRequest bootstrapStreamTableDirectory(File quickstartTmpDir)
      throws IOException {
    String tableName = "timeSeriesIndex";
    String directory = "examples/stream/timeSeriesIndex";
    File baseDir = new File(quickstartTmpDir, tableName);
    File dataDir = new File(baseDir, "rawdata");
    dataDir.mkdirs();

    // copyResourceTableToTmpDirectory(directory, tableName, baseDir, dataDir, true);

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    // Copy schema
    URL resource = classLoader.getResource(directory + File.separator + tableName + "_schema.json");
    Preconditions.checkNotNull(resource, "Missing schema json file for table - " + tableName);
    File schemaFile = new File(baseDir, tableName + "_schema.json");
    FileUtils.copyURLToFile(resource, schemaFile);

    // Copy table config
    File tableConfigFile = new File(baseDir, "timeSeriesIndex_realtime_table_config.json");
    String sourceTableConfig = directory + File.separator + "timeSeriesIndex_realtime_table_config.json";
    resource = classLoader.getResource(sourceTableConfig);
    Preconditions.checkNotNull(resource, "Missing table config file for table - " + tableName);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    // Copy raw data
    String sourceRawDataPath = directory + File.separator + "rawdata";
    resource = classLoader.getResource(sourceRawDataPath);
    if (resource != null) {
      File rawDataDir = new File(resource.getFile());
      if (rawDataDir.isDirectory()) {
        // Copy the directory from `pinot-tools/src/main/resources/examples` directory. This code path is used for
        // running Quickstart inside IDE, `ClassLoader.getResource()` should source it at build directory,
        // e.g. `/pinot-tools/target/classes/examples/batch/airlineStats/rawdata`
        FileUtils.copyDirectory(rawDataDir, dataDir);
      } else {
        // Copy the directory recursively from a jar file. This code path is used for running Quickstart using
        // pinot-admin script. The `ClassLoader.getResource()` should found the resources in the jar file then
        // decompress it, e.g. `lib/pinot-all-jar-with-dependencies.jar!/examples/batch/airlineStats/rawdata`
        String[] jarPathSplits = resource.toString().split("!/", 2);
        JarUtils.copyResourcesToDirectory(jarPathSplits[0], jarPathSplits[1], dataDir.getAbsolutePath());
      }
    } else {
      LOGGER.warn("Not found rawdata directory for table {} from {}", tableName, sourceRawDataPath);
    }

    return new QuickstartTableRequest(baseDir.getAbsolutePath());
  }
}
