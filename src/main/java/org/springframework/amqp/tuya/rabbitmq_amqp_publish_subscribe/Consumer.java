package org.springframework.amqp.tuya.rabbitmq_amqp_publish_subscribe;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @author alvarezlcamilo
 *
 */
public class Consumer extends DefaultConsumer {

	private static final boolean MULTIPLE_ACCEPT = false;

	private Channel channel;

	public Consumer(Channel channel, String queueName) {
		super(channel);
		this.channel = channel;
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			throws IOException {

		long deliveryTag = envelope.getDeliveryTag();

//		System.out.println("routingKey: " + envelope.getRoutingKey());
//		System.out.println("contentType: " + properties.getContentType());
//		System.out.println("deliveryTag: " + deliveryTag);

		String bodyContent = new String(body, StandardCharsets.UTF_8);
		System.out.printf("msg #%s: %s%n", deliveryTag, bodyContent);
		channel.basicAck(deliveryTag, MULTIPLE_ACCEPT);

		/*
		 * Retrieving Individual Messages ("Pull API") It is also possible to retrieve
		 * individual messages on demand ("pull API" a.k.a. polling). This approach to
		 * consumption is highly inefficient as it is effectively polling and
		 * applications repeatedly have to ask for results even if the vast majority of
		 * the requests yield no results. Therefore using this approach is highly
		 * discouraged.
		 */
//		GetResponse response = channel.basicGet(queueName, AUTO_ACK);
//		
//		if (response == null) {
//			System.out.println("No se encontraron mensajes en la cola: " + queueName);
//			return;
//		}
//		
//		System.out.println("Message count: " + response.getMessageCount());
//		byte[] responseBody = response.getBody();
//		String bodyContentResponse = new String(responseBody, StandardCharsets.UTF_8);
//		System.out.println("bodybodyContentResponse: " + bodyContentResponse);
//
//		channel.basicAck(deliveryTag, MULTIPLE_ACCEPT);
	}
}
