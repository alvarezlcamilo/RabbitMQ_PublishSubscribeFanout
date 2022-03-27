package org.springframework.amqp.tuya.rabbitmq_amqp_publish_subscribe;

import java.util.Map;
import java.util.Scanner;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author alvarezlcamilo
 *
 */
public class Publisher {

	private static final int CONNECTION_TIMEOUT = 30000;
	private static final String CONNECTION_USERNAME = "ptyqkvdj";
	private static final String CONNECTION_PASSWORD = "KHDSMA5mpLi7lEtj2c5tuCPMZPiNjAia";
	private static final String HOST = "shrimp.rmq.cloudamqp.com";
	private static final String VIRTUAL_HOST = "ptyqkvdj";

	private static final boolean DURABLE_QUEUE = false;
	private static final boolean EXCLUSIVE_QUEUE = false;
	private static final boolean AUTO_DELETE_QUEUE = false;

	private Channel channel;
	private Connection connection;

	public Publisher(String queueName, Map<String, Object> queueArguments) {
		super();

		String uri = System.getenv("CLOUDAMQP_URL");
		if (uri == null)
			uri = "amqps://" + CONNECTION_USERNAME + ":" + CONNECTION_PASSWORD + "@" + HOST + "/" + VIRTUAL_HOST;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(uri);
			factory.setConnectionTimeout(CONNECTION_TIMEOUT);
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclareNoWait(queueName, DURABLE_QUEUE, EXCLUSIVE_QUEUE, AUTO_DELETE_QUEUE, queueArguments);
		} catch (Exception e) {
			System.out.println("Error inicializando Publisher");
			e.printStackTrace();
		}
	}

	public void publish(String exchangeName, String message, String routingKey, BasicProperties messageProps) {

		try {
			channel.basicPublish(exchangeName, routingKey, messageProps, message.getBytes());
			System.out.println(" [x] Enviado '" + message + "'");
		} catch (Exception e) {
			System.out.println("Error publicando mensaje: " + message);
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			System.out.println("Error cerrando Channel y/o Connection desde el Publisher");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		Map<String, Object> queueArguments = null;
		String exchangeName = "";
		String queueName = "hello";
		Publisher publisher = new Publisher(queueName, queueArguments);

		String routingKey = "hello";
		String message = "Saludos Comunidad de Desarrolladores!";
		BasicProperties messageProps = null;

		Scanner scanner = new Scanner(System.in);
		while (!message.equals("fin")) {
			System.out.println("Envie mensaje o fin para terminar");
			message = scanner.nextLine();
			publisher.publish(exchangeName, message, routingKey, messageProps);
			System.out.println("Mensaje publicado en la cola: " + message);
		}
		scanner.close();
		publisher.close();
	}
}