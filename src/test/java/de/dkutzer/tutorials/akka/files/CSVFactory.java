package de.dkutzer.tutorials.akka.files;

import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.collect.Lists;

public class CSVFactory {

	public static void createCatalog(final String fName, final int count) throws IOException {
		final FileWriter fileWriter = new FileWriter(fName);
		final Object[] header = { "nr", "id", "name", "price", "cat1", "cat2", "stock" };
		final CSVPrinter printer = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withDelimiter(';'));
		printer.printRecord(header);

		for (int i = 0; i < count; i++) {

			final String id = UUID.randomUUID().toString();
			final String name = "product_" + i;
			final String price = RandomStringUtils.randomNumeric(3);
			final String cat1 = RandomStringUtils.randomAlphabetic(255);
			final String cat2 = RandomStringUtils.randomAlphabetic(1023);
			final String stock = RandomStringUtils.randomNumeric(2);
			printer.printRecord(Lists.newArrayList("" + i, id, name, price, cat1, cat2, stock));
		}
		printer.flush();
		printer.close();
	}

	public static void main(String[] args) throws IOException {
		// createCatalog("src/test/resources/cat_10.csv", 10);
		// createCatalog("src/test/resources/cat_1000.csv", 1000);
		// createCatalog("src/test/resources/cat_1_000_000.csv", 1_000_000);
		createCatalog("src/test/resources/cat_10_000_000.csv", 10_000_000);
	}

}
