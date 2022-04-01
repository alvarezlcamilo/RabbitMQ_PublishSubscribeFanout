package org.springframework.amqp.tuya.rabbitmq_amqp_publish_subscribe;

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
	private static final String CONNECTION_USERNAME = "comunidaddev.u";
	private static final String CONNECTION_PASSWORD = "comunidaddev.p";
	private static final String HOST = "lively-greyhound.rmq.cloudamqp.com";
	private static final String VIRTUAL_HOST = "integracion.vh";
	private static final String EXCHANGE_NAME = "notificaciones.fx";
	private static final String EXCHANGE_TYPE = "fanout";
	private static final boolean EXCHANGE_DURABLE = true;
//	private static final String CONNECTION_USERNAME = "ptyqkvdj";
//	private static final String CONNECTION_PASSWORD = "KHDSMA5mpLi7lEtj2c5tuCPMZPiNjAia";
//	private static final String HOST = "shrimp.rmq.cloudamqp.com";
//	private static final String VIRTUAL_HOST = "ptyqkvdj";

	private Channel channel;
	private Connection connection;

	public Publisher() {
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
			channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, EXCHANGE_DURABLE);
		} catch (Exception e) {
			System.out.println("Error inicializando Publisher");
			e.printStackTrace();
		}
	}

	/**
	 * @param message      Mensaje para publicar
	 * @param messageProps Propiedades del mensaje
	 */
	public void publish(String message, BasicProperties messageProps) {

		try {
			channel.basicPublish(EXCHANGE_NAME, "", messageProps, message.getBytes());
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

		Publisher publisher = new Publisher();

		String message = "";
		BasicProperties messageProps = null;

		Scanner scanner = new Scanner(System.in);
		while (!message.equals("fin")) {
			System.out.println("Envie mensaje o fin para terminar");
			message = scanner.nextLine();
			publisher.publish(message, messageProps);
			System.out.println("Mensaje publicado en la cola: " + message);
		}
		scanner.close();
		publisher.close();
	}
}