package de.dkutzer.tutorials.akka.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import akka.Done;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Handler1;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.RouteResult;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;

public class ProcessFileRouteHandler implements Handler1<String> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFileRouteHandler.class);

	private static final long serialVersionUID = 1L;

	private final ActorMaterializer materializer;

	private final String colName;

	private final Queue<Document> documentQueue;

	public ProcessFileRouteHandler(ActorSystem system, MongoClient mongoclient, final String dbName) {
		super();
		LOGGER.info("creating FileProcessor...for db: {}", dbName);
		materializer = ActorMaterializer.create(system);

		colName = "file_" + new Date().getTime();
		LOGGER.debug("collectionname: " + colName);

		this.documentQueue = new LinkedBlockingQueue<>();

		system.actorOf(Props.create(MongoActor.class, documentQueue, mongoclient, colName));
	}

	@Override
	public RouteResult apply(final RequestContext ctx, final String filename) {

		LOGGER.debug("apply for file: [{}]", filename);
		final File file = new File(filename);
		// read first line for headers
		final String[] header;
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
			final String firstLine = bufferedReader.readLine();
			header = firstLine.split(";");
		} catch (final IOException e) {

			LOGGER.error(e.getMessage(), e);
			return ctx.completeWithStatus(StatusCodes.BAD_REQUEST);
		}
		final Sink<ByteString, CompletionStage<Done>> convertAndStoreSink = Sink
				.<ByteString>foreach(chunk -> sinkMethod(chunk, header));

		LOGGER.debug("starting processing now...");
		FileIO.fromFile(file)
				.via(Framing.delimiter(ByteString.fromString("\r\n"), Integer.MAX_VALUE, FramingTruncation.ALLOW))
				.to(convertAndStoreSink).run(materializer);

		LOGGER.debug("returning asynchronous return value");
		return ctx.complete(colName);
	}

	private void sinkMethod(ByteString chunk, String[] header) throws IOException {
		// LOGGER.trace("sinking chunk");
		final String utf8String = chunk.utf8String();
		final CSVParser parser = CSVParser.parse(utf8String, CSVFormat.DEFAULT.withDelimiter(';').withHeader(header));
		// LOGGER.trace(utf8String);
		for (final CSVRecord csvRecord : parser) {
			insertToMongoQueue(new HashMap<>(csvRecord.toMap()));
		}

	}

	private void insertToMongoQueue(Map<String, Object> item) {

		final Document document = new Document(item);
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("item: [{}]", item.toString());
			LOGGER.trace("itemSize: {}", item.size());

			LOGGER.trace("document: [{}]", document.toString());
			LOGGER.trace("docSize: {}", document.size());
		}

		documentQueue.add(document);

	}

}
