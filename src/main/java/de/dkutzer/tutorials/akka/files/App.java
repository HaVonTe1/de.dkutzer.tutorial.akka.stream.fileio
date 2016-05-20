package de.dkutzer.tutorials.akka.files;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

import akka.actor.ActorSystem;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.values.Parameters;

/**
 * Hello world!
 *
 */
public class App extends HttpApp {

	private static final ActorSystem system = ActorSystem.create();
	private static final Config config = ConfigFactory.load();
	private final MongoClient mongoClient;

	public App() {
		super();
		mongoClient = connectToMongoDb();
	}

	private App(final MongoClient mcl) {
		super();
		mongoClient = mcl;
	}

	@Override
	public Route createRoute() {
		// This handler generates responses to `/hello?name=XXX` requests
		final Route helloRoute = handleWith1(Parameters.stringValue("name"), (ctx, name) ->

		ctx.complete("Hello " + name + "!")

		);

		// @formatter:off
		return route(get(
				pathSingleSlash()
						.route(complete(ContentTypes.TEXT_HTML_UTF8, "<html><body>Hello world!</body></html>")),
				path("ping").route(complete("PONG!")), path("hello").route(helloRoute),
				path("file").route(handleWith1(Parameters.stringValue("file"),
						new ProcessFileRouteHandler(system, mongoClient, Consts.DBNAME))))

		);
		// @formatter:on

	}

	public static void main(final String[] args) throws IOException {

		// HttpApp.bindRoute expects a route being provided by
		// HttpApp.createRoute
		final App app = new App();
		app.bindRoute("localhost", 8080, system);
		System.out.println("Type RETURN to exit");
		System.in.read();
		system.terminate();
		app.mongoClient.close();
	}

	private static MongoClient connectToMongoDb() {
		final List<ServerAddress> addresses = new ArrayList<>();
		final List<MongoCredential> creds = new ArrayList<>();
		final Config mongoConfig = config.getConfig("mongodb");
		final Config authConfig = mongoConfig.getConfig("auth");
		final ConfigObject repsetObj = mongoConfig.getObject("repset");
		repsetObj.forEach((k, v) -> {

			final Config srvCfg = v.atKey(k).getConfig(k);
			addresses.add(new ServerAddress(srvCfg.getString("host"), srvCfg.getInt("port")));
			final String password = authConfig.getString("password");
			final String username = authConfig.getString("user");
			if (authConfig != null && !username.isEmpty() && password != null && !password.isEmpty()) {
				final MongoCredential credential = MongoCredential.createCredential(username,
						mongoConfig.getString("dbName"), password.toCharArray());
				creds.add(credential);
			}

		});

		MongoClient mongoClient;
		final MongoClientOptions mongoClientOptions = MongoClientOptions.builder().build();
		if (creds.size() > 0) {
			mongoClient = new MongoClient(addresses, creds, mongoClientOptions);
		} else {
			mongoClient = new MongoClient(addresses, mongoClientOptions);

		}
		final String readPreference = mongoConfig.getString("readPreference");
		switch (readPreference) {
		case "primary": {
			mongoClient.setReadPreference(ReadPreference.primaryPreferred());
			break;
		}
		case "secondary": {
			mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
			break;
		}
		case "nearest": {
			mongoClient.setReadPreference(ReadPreference.nearest());
			break;
		}
		default:
			mongoClient.setReadPreference(ReadPreference.primaryPreferred());
		}
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		return mongoClient;

	}

}
