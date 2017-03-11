package org.rabix.executor.rest;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.glassfish.hk2.api.ServiceLocator;
import org.rabix.common.config.ConfigModule;
import org.rabix.executor.ExecutorModule;
import org.rabix.executor.rest.api.ExecutorHTTPService;
import org.rabix.executor.rest.api.impl.ExecutorHTTPServiceImpl;
import org.rabix.executor.service.ExecutorService;
import org.rabix.transport.backend.Backend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;
import com.squarespace.jersey2.guice.BootstrapUtils;

public class ExecutorRestEntry {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorRestEntry.class);

  private static String configDir = "/.bunny/config";

  public static void main(String[] commandLineArguments) throws Exception {
    final CommandLineParser commandLineParser = new DefaultParser();
    final Options posixOptions = createOptions();

    CommandLine commandLine;
    try {
      commandLine = commandLineParser.parse(posixOptions, commandLineArguments);

      File configDir = getConfigDir(commandLine, posixOptions);
      ServiceLocator locator = BootstrapUtils.newServiceLocator();

      ConfigModule configModule = new ConfigModule(configDir, null);
      Injector injector = BootstrapUtils.newInjector(locator,
          Arrays.asList(
              new ServletModule(), 
              new ExecutorModule(configModule), 
              new AbstractModule() {
                @Override
                protected void configure() {
                  bind(BackendRegister.class).in(Scopes.SINGLETON);
                  bind(ServerBuilder.class).in(Scopes.SINGLETON);
                  bind(ExecutorHTTPService.class).to(ExecutorHTTPServiceImpl.class).in(Scopes.SINGLETON);
                }
          }));

      BootstrapUtils.install(locator);
      

      BackendRegister backendRegister = injector.getInstance(BackendRegister.class);
      Backend backend = backendRegister.start();
      
      ExecutorService executorService = injector.getInstance(ExecutorService.class);
      executorService.initialize(backend);

      if (!Arrays.stream(commandLineArguments).anyMatch(p -> p.equals("headless"))) {
        Server server = injector.getInstance(ServerBuilder.class).build();
        try {
          server.start();
          server.join();
        } finally {
          server.destroy();
        }
      }
      
    } catch (ParseException e) {
      logger.error("Encountered exception while parsing using PosixParser.", e);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Create command line options
   */
  private static Options createOptions() {
    Options options = new Options();
    options.addOption("v", "verbose", false, "verbose");
    options.addOption("c", "configuration-dir", true, "configuration directory");
    options.addOption("h", "help", false, "help");
    return options;
  }

  private static File getConfigDir(CommandLine commandLine, Options options) throws IOException {
    String configPath = commandLine.getOptionValue("configuration-dir");
    if (configPath != null) {
      File config = new File(configPath);
      if (config.exists() && config.isDirectory()) {
        return config;
      } else {
        logger.debug("Configuration directory {} doesn't exist or is not a directory.", configPath);
      }
    }
    File config = new File("config");
    if (config.exists() && config.isDirectory()) {
      logger.debug("Configuration directory found localy.");
      return config;
    }
    String homeDir = System.getProperty("user.home");

    config = new File(homeDir, configDir);
    if (!config.exists() || !config.isDirectory()) {
      logger.info("Config directory doesn't exist or is not a directory");
    }
    return config;
  }

}
