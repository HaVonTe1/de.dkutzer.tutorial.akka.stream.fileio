package de.dkutzer.tutorials.akka.files;

import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Test;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;

import akka.actor.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestResponse;
import akka.http.javadsl.testkit.TestRoute;
import akka.testkit.JavaTestKit;
import mockit.Deencapsulation;

public class ProcessCSVToMongoTest extends JUnitRouteTest {

	// this is sick...but...it is still sick
	final static App app;
	static {
		AbstractEmbededMonogDbTest.setUp();
		AbstractEmbededMonogDbTest.mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		app = Deencapsulation.newInstance(App.class, AbstractEmbededMonogDbTest.mongoClient);

	}
	TestRoute appRoute = testRoute(app.createRoute());

	@AfterClass
	public static void close() {
		AbstractEmbededMonogDbTest.tearDown();
	}

	@Test
	public void test1() throws Exception {

		final int count = 1_000_001;
		final String fname = "cat_1_000_000.csv";
		// final int count = 11;
		// final String fname = "cat_10.csv";
		final ActorSystem actorSystem = Deencapsulation.getField(app, "system");
		new JavaTestKit(actorSystem) {
			{

				final TestResponse resp = appRoute.run(HttpRequest.GET("/file?file=src/test/resources/" + fname))
						.assertStatusCode(StatusCodes.OK);
				final String colName = resp.entityAsString();
				final MongoCollection<Document> collection = AbstractEmbededMonogDbTest.mongoClient
						.getDatabase(Consts.DBNAME).getCollection(colName);

				new AwaitCond(duration("360 second"), duration("100 millis")) {
					@Override
					protected boolean cond() {
						return collection.count() == count;
					}
				};

			}
		};

	}

}
