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
 * Simple reader which listens to a topic and prints incoming data
 */
public class SimpleNumberStreamReader {

	private static final String PRIME_NUMBER_STREAM = "prime-number-stream";
	
	public static void main(String args[]){
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final String topicName = args.length > 0 ? args[1] : PRIME_NUMBER_STREAM;
		final Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-prime-number-streamer");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		final KStreamBuilder streamBuilder = new KStreamBuilder();
		
		final KStream<Integer, Integer> primeNumStream = streamBuilder.stream(topicName);
		
		System.out.println("--------------------From Topic:"+topicName+"--------------------");
		
		primeNumStream.foreach(new ForeachAction<Integer, Integer>() {

			@Override
			public void apply(Integer key, Integer value) {
				System.out.println("This number just in: " + "Key: " + key + ", Value: " + value);
			}
			
		});
		
		final KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, streamsConfiguration);
		
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		
	    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
	
}
