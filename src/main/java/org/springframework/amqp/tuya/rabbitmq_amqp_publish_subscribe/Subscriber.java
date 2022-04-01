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
	private static final String EXCHANGE_NAME = "notificaciones.fx";
	private static final String EXCHANGE_TYPE = "fanout";
	private static final boolean EXCHANGE_DURABLE = true;
	private static final String CONNECTION_USERNAME = "comunidaddev.u";
	private static final String CONNECTION_PASSWORD = "comunidaddev.p";
	private static final String HOST = "lively-greyhound.rmq.cloudamqp.com";
	private static final String VIRTUAL_HOST = "integracion.vh";
//	private static final String CONNECTION_USERNAME = "ptyqkvdj";
//	private static final String CONNECTION_PASSWORD = "KHDSMA5mpLi7lEtj2c5tuCPMZPiNjAia";
//	private static final String HOST = "shrimp.rmq.cloudamqp.com";
//	private static final String VIRTUAL_HOST = "ptyqkvdj";

	private Connection connection;
	private Channel channel;

	public Subscriber(String queue, Map<String, Object> queueArguments) {

		String uri = System.getenv("CLOUDAMQP_URL");
		if (uri == null)
			uri = "amqps://" + CONNECTION_USERNAME + ":" + CONNECTION_PASSWORD + "@" + HOST + "/" + VIRTUAL_HOST;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(uri);
			factory.setConnectionTimeout(CONNECTION_TIMEOUT);
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, EXCHANGE_DURABLE);
		} catch (Exception e) {
			System.out.println("Error inicializando Subscriber");
			e.printStackTrace();
		}
	}

	public String subscribe() {

		String consumerTag = null;

		try {
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, EXCHANGE_NAME, "");

			Consumer consumer = new Consumer(channel, queueName);
			System.out.println(" [*] Esperando mensajes...");
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

		String queueName = "comunidaddev.q";
		Map<String, Object> queueArguments = null;
		Subscriber subscriber = new Subscriber(queueName, queueArguments);

		subscriber.subscribe();

//		if (null != consumerTag) {
//			subscriber.close(consumerTag);
//		}
	}
}