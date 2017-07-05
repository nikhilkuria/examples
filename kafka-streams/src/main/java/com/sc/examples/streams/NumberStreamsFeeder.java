package com.sc.examples.streams;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

/**
 * @author nikhilkuria
 * Stupid feeder which feeds integers to the stream with an interval of 1 sec
 */
public class NumberStreamsFeeder {

	private static final String NUMBER_STREAM = "simple-number-stream";

	public static void main(String args[]) {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		produceSampleInput(bootstrapServers);
	}

	private static void produceSampleInput(String bootstrapServers) {
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

		final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props);

		IntStream.range(1, 50)
			.mapToObj(val -> 
				new ProducerRecord<>(NUMBER_STREAM, val, val))
			.forEach((payload) -> {
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				producer.send(payload);
			});
		
		producer.flush();

		producer.close();

	}

}
