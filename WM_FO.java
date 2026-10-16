import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class WM_FO {
    /**
     * Método que lee el fichero de los servicios a solicitar y se encarga del procesamiento y envío
     * de cada solicitud. Cada línea del archivo tendrá el código de la WS que se quiera activar.
     */
    private static void envioSolicitudes(String ipBroker, String puertoBroker, String idOperador, String servicios) {
        try (BufferedReader reader = new BufferedReader(new FileReader(servicios))) {
            String linea;
            String topic = "fo_central";

            // ---------------------------------------
            // CREACIÓN DEL PRODUCTOR DE KAFKA:
            // ---------------------------------------
            String bootstrapServers = ipBroker + ":" + puertoBroker;

            // Se establecen las propiedades:
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Se crea el productor:
            KafkaProducer<String, String> productor = new KafkaProducer<>(props);

            // Se lee cada solicitud de servicio del fichero:
            while ((linea = reader.readLine()) != null) {
                System.out.println("Leyendo solicitud de servicio: " + linea);

                // Se prepara la trama y la publicación en el topic (cola?):
                String mensaje = idOperador + "#" + linea;
                ProducerRecord<String, String> registro = new ProducerRecord<>(topic, idOperador, mensaje);

                // Se envía el mensaje de forma asíncrona, comprobando si ha fallado el envío o no:
                productor.send(registro, (metadata, ex) -> {
                    if (ex == null) {
                        System.out.println("Solicitud enviada con éxito a Central");
                    } else {
                        System.err.println("Ha fallado el envío de la solicitud");
                    }
                });

                // Se cierra la conexión:
                productor.flush(); // Envía todos los registros acumulados en el búfer
                Thread.sleep(4000); // Pausa de 4 segundos
            }

            productor.close();

        } catch (IOException e) {
            System.err.println("Error durante la lectura del archivo: " + e.getMessage());
        
        } catch (InterruptedException e) {
            System.err.println("Error durante la espera: " + e.getMessage());
        }
    }


    /**
     * Función principal.
     */
    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 3 || args.length > 4) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas y el ID del operador");
			System.out.println("$./WM_FO ip_broker puerto_broker id_operador [fichero_servicios]");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String idOperador = args[2];

        // Si se ha introducido un fichero de solicitud de servicios:
        if (args.length == 4) {
            String servicios = args[3];
        }

        System.out.println("+-----------------------------+");
        System.out.println("|       FIELD OPERATORS       |");
        System.out.println("+-----------------------------+\n");

        System.out.println("Iniciando la aplicación...");
        System.out.println("ID del operador: " + idOperador);
        System.out.println("Conectando al Broker del gestor de colas en: " + ipBroker + ":" + puertoBroker);

    }
}