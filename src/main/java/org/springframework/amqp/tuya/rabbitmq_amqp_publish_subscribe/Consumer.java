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

		String bodyContent = new String(body, StandardCharsets.UTF_8);
		System.out.printf("msg #%s: %s%n", deliveryTag, bodyContent);
		channel.basicAck(deliveryTag, MULTIPLE_ACCEPT);
	}
}
