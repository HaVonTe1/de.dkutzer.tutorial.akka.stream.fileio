package de.dkutzer.tutorials.akka.files;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

import akka.actor.UntypedActor;
import scala.concurrent.duration.Duration;

public class MongoActor extends UntypedActor {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoActor.class);

	private final Queue<Document> queue;
	private final MongoClient mongoClient;
	private final MongoCollection<Document> collection;

	public MongoActor(Queue<Document> queue, MongoClient client, String colName) {
		super();
		this.queue = queue;
		this.mongoClient = client;
		collection = mongoClient.getDatabase(Consts.DBNAME).getCollection(colName);

		getContext().system().scheduler().schedule(Duration.create(1, TimeUnit.SECONDS),
				Duration.create(3, TimeUnit.SECONDS), this.getSelf(), Boolean.TRUE, getContext().dispatcher(),
				getSelf());
	}

	@Override
	public void onReceive(Object arg0) throws Exception {

		final List<Document> docs = Lists.newArrayList();

		while (!queue.isEmpty() && docs.size() < 100000) {

			docs.add(queue.poll());
		}
		insert(docs);
	}

	public int insert(List<Document> docs) {

		if (docs.size() == 0) {
			return 0;
		}
		collection.insertMany(docs);

		// LOGGER.debug("inserting now: {}", docs.size());
		//
		// final List<InsertOneModel<Document>> requests =
		// docs.parallelStream().map(item -> {
		// return new InsertOneModel<>(item);
		// }).collect(Collectors.toList());
		//
		// List<com.mongodb.bulk.BulkWriteError> writeErrors = null;
		// BulkWriteResult result = null;
		// try {
		// result = collection.bulkWrite(requests, new
		// BulkWriteOptions().ordered(false));
		// } catch (final MongoBulkWriteException bwe) {
		// bwe.printStackTrace();
		// writeErrors = bwe.getWriteErrors();
		// result = bwe.getWriteResult();
		//
		// for (final com.mongodb.bulk.BulkWriteError e : writeErrors) {
		// LOGGER.error(e.getMessage());
		// LOGGER.error(e.getCode() + "");
		// LOGGER.error(e.getDetails().toString());
		// LOGGER.error(e.getIndex() + " failed");
		// }
		//
		// LOGGER.error(result.toString());
		// throw new IllegalStateException(bwe);
		// }
		// LOGGER.debug("done");
		return docs.size();
	}
}
