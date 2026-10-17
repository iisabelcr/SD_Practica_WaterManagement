import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class WM_FO {
    /**
     * Función que limpia la pantalla de la consola
     */
    private static void clear() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }


    /**
     * Procedimiento que muestra mensajes de iniciación del programa.
     * @param ipBroker
     * @param puertoBroker
     * @param idOperador
     * @throws InterruptedException
     */
    private static void iniciarAplicacion(String ipBroker, String puertoBroker, String idOperador) throws InterruptedException {
        System.out.println("Iniciando la aplicación...");
        Thread.sleep(2000);
        System.out.println("ID del operador: " + idOperador);
        Thread.sleep(2000);
        System.out.println("Conectando al Broker del gestor de colas en: " + ipBroker + ":" + puertoBroker);
        Thread.sleep(2000);
    }


    /**
     * Función que muestra y procesa el menú.
     * @return opcion: número de la opción escogida en el menú
     */
    private static int mostrarMenu(Scanner scanner) {    
        System.out.println("+-----------------------------+");
        System.out.println("|       FIELD OPERATORS       |");
        System.out.println("+-----------------------------+\n");

        System.out.println("1. Procesar fichero de solicitudes de riego");
        System.out.println("2. Solicitar suministro");
        System.out.println("3. Ver estaciones de riego disponibles\n");

        int opcion = -1;
        boolean correcto = false; 

        // Se seguirá pidiendo la opción hasta que se inserte un número correcto
        while (!correcto) {
            try {
                System.out.println("Selecciona una opción: ");
                opcion = Integer.parseInt(scanner.nextLine());

                if (opcion >= 1 && opcion <= 3) {
                    correcto = true;
                } else {
                   System.out.println("No existe esa opción"); 
                }
            } catch (NumberFormatException e) {
                System.out.println("La opción debe ser numérica");
            }
        }

        return opcion;
    }
    

    /**
     * Método que lee el fichero de los servicios a solicitar y se encarga del procesamiento y envío
     * de cada solicitud. Cada línea del archivo tendrá el código de la WS que se quiera activar.
     * @param ipBroker
     * @param puertoBroker
     * @param idOperador
     * @param servicios
    */
    private static void ficheroSolicitudes(String ipBroker, String puertoBroker, String idOperador, String servicios) {
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

                // Se prepara la trama y la publicación en la cola:
                String mensaje = idOperador + "/" + linea;
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
     * @param args
     */
    public static void main(String[] args) {
        // ---------------------------------------
        // VALIDACIÓN DE ARGUMENTOS:
        // ---------------------------------------
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

        Scanner scanner = new Scanner(System.in); // Para la lectura de datos por consola
        scanner.close(); // Se cierra el scanner

    }
}