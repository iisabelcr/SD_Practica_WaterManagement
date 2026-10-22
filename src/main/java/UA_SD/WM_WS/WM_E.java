package UA_SD.WM_WS;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class WM_E {
    /**
     * Topic de la comuncicación a través de Kafka desde el Engine a Central.
     */
    private static final String TOPIC_PRODUCTOR = "engine_central";

    /**
     * Topic de la comuncicación a través de Kafka desde Central al Engine.
     */
    private static final String TOPIC_CONSUMIDOR = "central_engine";

    /**
     * Atributo de la clase que determina si la WS está averiada
     */
    static boolean averia = false; // Determina si la WS está averiada


    
    /**
     * Clase interna que permite gestionar la lógica de provocar averías en un hilo distinto al principal
     * HiloKO
     */
    static class HiloKO extends Thread {
        @Override
        public void run() {
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.println("Pulsa cualquier tecla para provocar una avería manual: ");
                scanner.nextLine(); // El hilo se queda parado hasta que se pulse una tecla
                averia = true;

                System.out.println("Estación averiada, pulsa cualquier tecla para arreglarla: ");
                scanner.nextLine(); // El hilo se queda parado hasta que se pulse una tecla
                averia = false;
            }
        }
    }



    /**
     * Clase interna que permite separar la comunicación desde Central al Engine en un hilo aparte.
     * HiloConsumidorKafka
     */
    static class HiloConsumidorKafka extends Thread {
        private String ipBroker;
        private String puertoBroker;

        public HiloConsumidorKafka(String ipBroker, String puertoBroker) {
            this.ipBroker = ipBroker;
            this.puertoBroker = puertoBroker;
        }

        @Override
        public void run() {
            // Creamos el consumidor:
            KafkaConsumer<String, String> consumidor = crearConsumidor(ipBroker, puertoBroker);
            
            while (true) {
                // Recibimos el mensaje por parte de Central (o varios mensajes):
                ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofSeconds(1));

                // Procesamos cada mensaje:
                for (ConsumerRecord<String, String> registro : registros) {
                    String mensaje = registro.value();
                    System.out.println("Orden recibida desde Central: " + mensaje);

                    String[] info = mensaje.split("/");
                    String operacion = info[0]; // Extraemos el código de la operación

                    if (operacion.equals("SOLICITUD_RIEGO")) {
                        // AQUI INICIO EL RIEGO Y TAL
                    }
                    
                }
            }

            //consumidor.close(); // Se cierra el topic
        }
    }



    /**
     * Función que limpia la pantalla de la consola.
     */
    private static void clear() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }



    /**
     * Procedimiento que muestra mensajes de iniciación del programa.
     */
    private static void iniciarEngine() {
        try {
            System.out.println("Iniciando el Engine de la Estación de riego...");
            Thread.sleep(2000);
            System.out.println("Estableciendo conexión con el Monitor...");
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Fallo al iniciar la aplicación");
            System.err.println("Error: " + e.getMessage());
        }
    }


    
    /**
     * Función que lee la información procedente del socket.
     * @param socket
     * @return datos
     */
    private static String leerSocket(Socket socket) {
		try {
			InputStream aux = socket.getInputStream();
			DataInputStream flujo = new DataInputStream(aux);
			return flujo.readUTF();

		} catch (Exception e) {
			System.out.println("Ha fallado la conexión con Central");
            System.out.println("Error: " + e.getMessage());
            return null;
		}
	}



	/**
     * Método que escribe la información dentro del socket.
     * @param socket
     * @param datos
     */
	private static void escribirSocket(Socket socket, String datos) {
		try {
			OutputStream aux = socket.getOutputStream();
			DataOutputStream flujo = new DataOutputStream(aux);
			flujo.writeUTF(datos);

		} catch (Exception e) {
			System.out.println("Ha fallado la conexión con Central");
            System.out.println("Error: " + e.getMessage());
		}
	}



    /**
     * Función que crea el consumidor las comunicaciones Central-FO con los topics de Kafka.
     * @param ipBroker
     * @param puertoBroker
     * @param idOperador
     * @return consumidor
     */
    private static KafkaConsumer<String, String> crearConsumidor(String ipBroker, String puertoBroker) {
        String bootstrapServers = ipBroker + ":" + puertoBroker;

        // Se establecen las propiedades:
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-engine");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Para que solo lea los mensajes nuevos
    
        // Se crea el consumidor:
        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);

        // Suscribir el consumidor a la cola donde Central responde:
        consumidor.subscribe(Collections.singletonList(TOPIC_CONSUMIDOR));

        return consumidor;
    }



    /**
     * Función principal.
     * @param args
     */
    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 4) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas y la IP y puerto del monitor");
			System.out.println("$./WM_WS_E ip_broker puerto_broker ip_monitor puerto_monitor");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String ipMonitor = args[2];
        String puertoMonitor = args[3];

        // ----------------------------------------------
        // INICIAR HILO PARA PROVOCAR UNA AVERÍA MANUAL:
        // ----------------------------------------------
        HiloKO hiloKO = new HiloKO();
        hiloKO.start();

        // ----------------------------------------------------------------
        // INICIAR HILO PARA LA COMUNICACIÓN ENTRE LA CENTRAL Y EL ENGINE:
        // ----------------------------------------------------------------
        HiloConsumidorKafka hiloKafka = new HiloConsumidorKafka(ipBroker, puertoBroker);
        hiloKafka.start();    

        // ------------------------
        // CONEXIÓN CON EL MONITOR:
        // ------------------------
        try {
            iniciarEngine();
            
            Socket socketMonitor = new Socket(ipMonitor, Integer.parseInt(puertoMonitor));
            
            // Bucle infinito que espera la comprobación de estado de salud por parte del Monitor
            while (true) {
                String respuesta = leerSocket(socketMonitor);

                // No es necesario comprobar el mensaje, el Monitor siempre mandará al Engine un mensaje de STATUS:
                if (respuesta != null) {
                    if (!averia) {
                        escribirSocket(socketMonitor, "OK");

                    } else {
                        escribirSocket(socketMonitor, "KO");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Ha fallado la conexión con el Monitor");
            System.out.println("Error: " + e.getMessage());
        }
    }
}

// COSAS QUE FALTAN:
// Hacer función para iniciar el riego
// Gestionar el productor para mandar mensajes a central