package org.springframework.amqp.tuya.rabbitmq_amqp_publish_subscribe;

import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author alvarezlcamilo
 *
 */
public class Subscriber {

	private static final boolean AUTO_ACK = false;
	private static final int CONNECTION_TIMEOUT = 30000;
	private static final boolean DURABLE_QUEUE = false;
	private static final boolean EXCLUSIVE_QUEUE = false;
	private static final boolean AUTO_DELETE_QUEUE = false;
	private static final String CONNECTION_USERNAME = "ptyqkvdj";
	private static final String CONNECTION_PASSWORD = "KHDSMA5mpLi7lEtj2c5tuCPMZPiNjAia";
	private static final String HOST = "shrimp.rmq.cloudamqp.com";
	private static final String VIRTUAL_HOST = "ptyqkvdj";

	private Connection connection;
	private Channel channel;
	private String queueName;

	public Subscriber(String queue, Map<String, Object> queueArguments) {

		this.queueName = queue;

		String uri = System.getenv("CLOUDAMQP_URL");
		if (uri == null)
			uri = "amqps://" + CONNECTION_USERNAME + ":" + CONNECTION_PASSWORD + "@" + HOST + "/" + VIRTUAL_HOST;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(uri);
			factory.setConnectionTimeout(CONNECTION_TIMEOUT);
			connection = factory.newConnection();

			channel = connection.createChannel();
			channel.queueDeclare(queueName, DURABLE_QUEUE, EXCLUSIVE_QUEUE, AUTO_DELETE_QUEUE, queueArguments);
		} catch (Exception e) {
			System.out.println("Error inicializando Subscriber");
			e.printStackTrace();
		}
	}

	public String subscribe() {

		System.out.println(" [*] Esperando mensajes...");
		String consumerTag = null;

		try {
			Consumer consumer = new Consumer(channel, queueName);
			consumerTag = channel.basicConsume(queueName, AUTO_ACK, consumer);
		} catch (Exception e) {
			System.out.println("Error configurando el Consumer");
			e.printStackTrace();
		}
		return consumerTag;
		
	}

	public void close(String consumerTag) {
		try {
			channel.basicCancel(consumerTag);
			channel.close();
			connection.close();
		} catch (Exception e) {
			System.out.println("Error cerrando Channel y/o Connection desde el Subscriber");
			e.printStackTrace();
		}
		
	}

	public static void main(String[] argv) throws Exception {

		String queueName = "hello";
		Map<String, Object> queueArguments = null;
		Subscriber subscriber = new Subscriber(queueName, queueArguments);

		String consumerTag = subscriber.subscribe();

//		if (null != consumerTag) {
//			subscriber.close(consumerTag);
//		}
	}
}