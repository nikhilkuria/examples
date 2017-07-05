package com.sc.examples.streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * @author nikhilkuria
 * Simple number stream.
 * From the incoming stream of numbers
 * Sieves out the prime numbers and pushed them to another topic
 * Prerequ is the existence of the two topics
 */
public class NumberStreams {

	private static final String PRIME_NUMBER_STREAM = "prime-number-stream";
	private static final String NUMBER_STREAM = "simple-number-stream";

	public static final void main(String args[]) {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-number-streamer");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final KStreamBuilder streamBuilder = new KStreamBuilder();

		final KStream<Integer, Integer> numberStream = streamBuilder.stream(NUMBER_STREAM);

		numberStream.foreach(new ForeachAction<Integer, Integer>() {

			@Override
			public void apply(Integer key, Integer value) {
				System.out.println("This just in: " + "Key: " + key + ", Value: " + value);
			}

		});

		// Filter out prime numbers
		// Now send them to another topic

		final KStream<Integer, Integer> primeNumbersStream = numberStream.filter((key, value) -> {
			return isPrime(value);
		});

		primeNumbersStream.to(PRIME_NUMBER_STREAM);

		final KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, streamsConfiguration);
		kafkaStreams.cleanUp();
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

	}

	private static boolean isPrime(Integer value) {
		// check if n is a multiple of 2
		if (value % 2 == 0)
			return false;
		// if not, then just check the odds
		for (int i = 3; i * i <= value; i += 2) {
			if (value % i == 0)
				return false;
		}
		return true;
	}

}
