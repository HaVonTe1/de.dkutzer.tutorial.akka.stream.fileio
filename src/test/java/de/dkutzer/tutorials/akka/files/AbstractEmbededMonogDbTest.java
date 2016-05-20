package de.dkutzer.tutorials.akka.files;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.runtime.Network;

public class AbstractEmbededMonogDbTest {

	public static final String DB_TESTNAME = "test";

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEmbededMonogDbTest.class);
	private static final Logger mongoLOGGER = LoggerFactory.getLogger("embeddedMongo");

	protected static MongodExecutable mongodExecutable = null;

	protected static MongodProcess mongod = null;

	protected static MongoClient mongoClient;

	protected static IMongodConfig mongodConfig;

	/**
	 * Sets the up.
	 */
	@BeforeClass
	public static void setUp() {

		// sometime the embeddes mongo starts with a locked port and fails.
		// we try to setup the mongo 3 times before failing...
		int count = 0;
		final int maxTries = 3;
		while (true) {

			try {
				final String embedMongoPort = System.getProperty("embedMongoPort");
				if (embedMongoPort == null) {
					mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION).build();

				} else {
					mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION)
							.net(new Net(Integer.parseInt(embedMongoPort), Network.localhostIsIPv6())).build();

				}

				final IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
						.defaultsWithLogger(Command.MongoD, mongoLOGGER).build();
				final MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);

				mongodExecutable = runtime.prepare(mongodConfig);
				mongod = mongodExecutable.start();

				mongoClient = new MongoClient(
						new ServerAddress(mongodConfig.net().getServerAddress(), mongodConfig.net().getPort()));
				LOGGER.info("Embedded Mongo client it's running on host {} port {}.",
						mongodConfig.net().getServerAddress(), mongodConfig.net().getPort());

				break;
			} catch (final IOException e) {
				try {
					Thread.sleep(250);
				} catch (final InterruptedException e1) {
				}
				if (++count == maxTries) {
					Assert.fail(e.getMessage());
				}
			}
		}
	}

	protected static void dropDatabase(String dbName) {
		if (mongoClient != null) {
			mongoClient.dropDatabase(dbName);
		}
	}

	/**
	 * Tear down.
	 */
	@AfterClass
	public static void tearDown() {
		if (mongoClient != null) {
			mongoClient.close();
		}

		if (mongod != null) {
			mongod.stop();
		}
		if (mongodExecutable != null) {
			mongodExecutable.stop();
		}
	}

}
