package com.transglobe.streamingetl.automation;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AutomationTask {
	private static final Logger LOG = LoggerFactory.getLogger(AutomationTask.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final int KAFKA_REST_PORT = 9101;
	private static final int LOGMINER_REST_PORT = 9102;
	private static final int HEALTH_REST_PORT = 9103;
	private static final int PARTYCONTACT_REST_PORT = 9201;
	
	@Value("${kafka.rest.home}")
	private String kafkaRestHome;
	
	@Value("${kafka.rest.start.script}")
	private String kafkaRestStartScript;
	
	private AtomicBoolean systemStarting = new AtomicBoolean(false);

	private AtomicBoolean systemStarted = new AtomicBoolean(false);
	
	private Process kafkaRestStartProcess;

	private ExecutorService kafkaRestStartExecutor;

	@Scheduled(fixedRate = 60000, initialDelay=5000)
	public void startSystem() throws Exception  {
		Calendar calendar = Calendar.getInstance();
		LOG.info(">>>>>>>>>>>> startSystem, The time is now {}", dateFormat.format(calendar.getTime()));

		try {

			// check if system is starting
			if (systemStarting.get() || systemStarted.get()) return;

			// set system starting true
			systemStarting.set(true);

			LOG.info(">>>>> Kill servers");
			killServers();
			LOG.info(">>>>> [OK] no servers is runnig or servers are killed successfully");

			LOG.info(">>>>> Start Kafka Server");
			boolean serverStarted = startRestServer(kafkaRestHome, kafkaRestStartScript, KAFKA_REST_PORT);
			if (serverStarted) {
				LOG.info(">>>>> [OK] Kafka Server Started.");
			}
			
			// set system started true
			systemStarted.set(true);

			// set system starting false
			systemStarting.set(false);
		} catch (Exception e) {
			LOG.error(">>> Error!!!, startSystem, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {

		}
	}
	public void killServers() throws Exception {
		boolean listening = false;
		// kill process of kafka-rest
		listening = checkPortListening(KAFKA_REST_PORT);
		LOG.info(">>>>>>>port {} is listening:{}", KAFKA_REST_PORT,listening);
		if (listening) {
			killProcess(KAFKA_REST_PORT);

			// check 
			listening = checkPortListening(KAFKA_REST_PORT);
			if (listening) {
				throw new Exception("Port:" + KAFKA_REST_PORT + " cannot be killed.!!!");
			}
		}

		// kill process of kafka-rest
		listening = checkPortListening(LOGMINER_REST_PORT);
		LOG.info(">>>>>>>port {} is listening:{}", LOGMINER_REST_PORT, listening);
		if (listening) {
			killProcess(LOGMINER_REST_PORT);
			// check 
			listening = checkPortListening(LOGMINER_REST_PORT);
			if (listening) {
				throw new Exception("Port:" + LOGMINER_REST_PORT + " cannot be killed.!!!");
			}
		}

		// kill process of kafka-rest
		listening = checkPortListening(HEALTH_REST_PORT);
		LOG.info(">>>>>>>port {} is listening:{}", HEALTH_REST_PORT, listening);
		if (listening) {
			killProcess(HEALTH_REST_PORT);
			// check 
			listening = checkPortListening(HEALTH_REST_PORT);
			if (listening) {
				throw new Exception("Port:" + HEALTH_REST_PORT + " cannot be killed.!!!");
			}
		}

		// kill process of kafka-rest
		listening = checkPortListening(PARTYCONTACT_REST_PORT);
		LOG.info(">>>>>>>port {} is listening:{}", PARTYCONTACT_REST_PORT, listening);
		if (listening) {
			killProcess(PARTYCONTACT_REST_PORT);
			// check 
			listening = checkPortListening(PARTYCONTACT_REST_PORT);
			if (listening) {
				throw new Exception("Port:" + PARTYCONTACT_REST_PORT + " cannot be killed.!!!");
			}
		}
	}
	public void killProcess(int port) throws Exception {
		ProcessBuilder builder = new ProcessBuilder();
		String script = String.format("kill -9 $(lsof -t -i:%d -sTCP:LISTEN)", port);
		LOG.info(">>> stop script={}", script);

		builder.command("bash", "-c", script);
		builder.directory(new File("."));

		Process killProcess = builder.start();

		int exitVal = killProcess.waitFor();
		if (exitVal == 0) {
			LOG.info(">>> Kill Port:{} Success!!! ", port);
		} else {
			LOG.error(">>> Kill Port:{}  Error!!!  exitcode={}", port, exitVal);

		}
		if (killProcess.isAlive()) {
			killProcess.destroy();
		}

	}
	public boolean checkPortListening(int port) throws Exception {
		LOG.info(">>>>>>>>>>>> checkPortListening:{} ", port);

		BufferedReader reader = null;
		try {
			ProcessBuilder builder = new ProcessBuilder();
			String script = "netstat -tnlp | grep :" + port;
			builder.command("bash", "-c", script);
			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

			builder.directory(new File("."));
			Process checkPortProcess = builder.start();

			AtomicBoolean portRunning = new AtomicBoolean(false);


			int exitVal = checkPortProcess.waitFor();
			if (exitVal == 0) {
				reader = new BufferedReader(new InputStreamReader(checkPortProcess.getInputStream()));
				reader.lines().forEach(line -> {
					if (StringUtils.contains(line, "LISTEN")) {
						portRunning.set(true);
						LOG.info(">>> Success!!! portRunning.set(true)");
					}
				});
				reader.close();

				LOG.info(">>> Success!!! portRunning={}", portRunning.get());
			} else {
				LOG.error(">>> Error!!!  exitcode={}", exitVal);


			}
			if (checkPortProcess.isAlive()) {
				checkPortProcess.destroy();
			}

			return portRunning.get();
		} finally {
			if (reader != null) reader.close();
		}



	}
	public boolean startRestServer(String path, String script, int port) throws Exception {
		LOG.info(">>>>>>>>>>>> checkstartRestServer, {}, {}", path, script);
		BufferedReader reader = null;
		try {
			ProcessBuilder builder = new ProcessBuilder();
			builder.command("bash", "-c", script + " &");
			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

			builder.directory(new File(path));
			Process startRestServerProcess = builder.start();

			AtomicBoolean running = new AtomicBoolean(false);
			
//			ExecutorService kafkaStartExecutor = Executors.newSingleThreadExecutor();
//			kafkaStartExecutor.submit(new Runnable() {
//
//				@Override
//				public void run() {
//					BufferedReader reader = new BufferedReader(new InputStreamReader(startRestServerProcess.getInputStream()));
//					reader.lines().forEach(line -> {
//						LOG.info(line);
//						if (StringUtils.contains(line, "Started Application")) {
//							running.set(true);
//							return;
//						}  
//					});
//				}
//
//			});
			
			
			int exitVal = startRestServerProcess.waitFor();
			if (exitVal == 0) {
				LOG.info(">>> startRestServer exitcode={}", exitVal);
			} else {
				LOG.error(">>> startRestServer Error!!!  exitcode={}", exitVal);

			}
			
			
//			while (!running.get()) {
//				LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
//				Thread.sleep(10000);
//			}
			return running.get();

		} finally {
			if (reader != null) reader.close();
		}

	}
}
