package UA_SD.WM_FO;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class WM_FO {
    private static final String TOPIC_PRODUCTOR = "fo_central";
    private static final String TOPIC_CONSUMIDOR = "central_fo";

    /**
     * Clase interna que permite escuchar los mensajes de Central en un hilo distinto
     * HiloConsumidor
     */
    static class HiloConsumidor extends Thread {
        private final KafkaConsumer<String, String> consumidor;
        private final String idOperador;
        private final CountDownLatch listo = new CountDownLatch(1); // Contador regresivo para hilos
        private final BlockingQueue<String[]> eventos = new LinkedBlockingQueue<>(); // Lista segura para usar desde varios hilos (este y main). De esta forma, el main sabrá cuando acaba una solicitud
        private volatile boolean activo = true; // Hay que ponerle el atributo volatile porque dos hilos van a usarlo: este y main

        public HiloConsumidor(KafkaConsumer<String, String> consumidor, String idOperador) {
            this.consumidor = consumidor;
            this.idOperador = idOperador;
            setDaemon(true);
        }

        public CountDownLatch getListo() {
            return listo;
        }

        public BlockingQueue<String[]> getEventos() {
            return eventos;
        }

        public void detener() {
            activo = false;
            consumidor.wakeup(); // Única llamada segura desde otro hilo
        }

        @Override
        public void run() {
            try {
                while (activo) {
                    ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofSeconds(1));

                    // Cuando ya hay particiones asignadas, fijamos la posición y avisamos al main:
                    if (listo.getCount() > 0 && !consumidor.assignment().isEmpty()) {
                        for (TopicPartition tp : consumidor.assignment()) {
                            consumidor.position(tp); // fuerza a resolver el "latest"
                        }
                        listo.countDown();
                    }

                    for (ConsumerRecord<String, String> registro : registros) {
                        if (!idOperador.equals(registro.key())) continue; // Se comprueba si el mensaje va para esta FO
                        procesar(registro.value());
                    }
                }
            } catch (WakeupException e) {
                // Cierre normal, no es un erro
            } catch (Exception e) {
                System.err.println("Error en el consumidor: " + e.getMessage());
            } finally {
                consumidor.close();
            }
        }

        private void procesar(String mensaje) {
            String[] info = mensaje.split("/");
            if (mensaje.isEmpty()) return; // Si el mensaje no es válido, se descarta

            String operacion = info[0];

            switch (operacion) {
                // Mensajes que envía Central para notificar de lo que va sucediendo:
                case "WATERING_PROGRESS":
                    if (info.length > 2) System.out.println("[Central] " + info[2]);
                    break;
                
                    // Si acepta o deniega la solicitud de riego:
                case "WATERING_RESPONSE":
                    if (info.length > 2 && info[2].equals("OK")) {
                        System.out.println("Riego autorizado en " + info[1]);
                    } else {
                        System.out.println("Riego denegado en " + info[1] + (info.length > 3 ? ": " + info[3] : ""));
                        eventos.add(info);
                    }
                    break;
                
                // Los datos que va enviando, a tiempo real, la Central sobre la WS:
                case "WATERING_DATA": 
                    if (info.length >= 4) {
                        System.out.println(info[1] + " -> caudal: " + info[2] + " L/min | volumen: " + info[3] + " L");
                    }
                    break;

                // Indica que el riego de una estación ha llegado a su fin:
                case "WATERING_END": System.out.println("Riego finalizado en " + info[1]); eventos.add(info); break;

                // Alerta de fuga:
                case "LEAK_ALERT": System.out.println("¡FUGA en " + info[1] + "! Riego interrumpido."); eventos.add(info); break;

                // Fuga resuelta:
                case "LEAK_RESOLVED": System.out.println("Fuga resuelta en " + info[1]); break;

                // Envía la lista de estaciones disponibles:
                case "STATIONS_LIST": eventos.add(info); break;
                default: return;
            }
        }
    }

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
     */
    private static void iniciarAplicacion(String ipBroker, String puertoBroker, String idOperador) {
        try {
            System.out.println("Iniciando la aplicación...");
            Thread.sleep(1500);
            System.out.println("ID del operador: " + idOperador);
            Thread.sleep(1500);
            System.out.println("Conectando al Broker del gestor de colas en: " + ipBroker + ":" + puertoBroker);
            Thread.sleep(1500);
        } catch (InterruptedException e)  {
            System.err.println("Fallo al iniciar la aplicación");
        }
        
    }



    /**
     * Función que muestra y procesa el menú.
     * @return opción escogida
     */
    private static int mostrarMenu(Scanner scanner) {
        System.out.println("+-----------------------------+");
        System.out.println("|       FIELD OPERATORS       |");
        System.out.println("+-----------------------------+\n");

        System.out.println("1. Procesar fichero de solicitudes de riego");
        System.out.println("2. Solicitar suministro");
        System.out.println("3. Ver estaciones de riego disponibles");
        System.out.println("4. Salir\n");

        int opcion = -1;
        boolean correcto = false;

        // Se seguirá pidiendo la opción hasta que se inserte un número correcto
        while (!correcto) {
            try {
                System.out.println("Selecciona una opción: ");
                opcion = Integer.parseInt(scanner.nextLine());

                if (opcion >= 1 && opcion <= 4) {
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
     * Función que crea el productor para las comunicaciones FO-Central con los topics de Kafka.
     * @param ipBroker
     * @param puertoBroker
     * @return productor
     */
    private static KafkaProducer<String,String> crearProductor(String ipBroker, String puertoBroker) {
        String bootstrapServers = ipBroker + ":" + puertoBroker;

        // Se establecen las propiedades:
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Se crea el productor:
        KafkaProducer<String, String> productor = new KafkaProducer<>(props);

        return productor;
    }



    /**
     * Función que crea el consumidor las comunicaciones Central-FO con los topics de Kafka
     * @param ipBroker
     * @param puertoBroker
     * @param idOperador
     * @return consumidor
     */
    private static KafkaConsumer<String, String> crearConsumidor(String ipBroker, String puertoBroker, String idOperador) {
        String bootstrapServers = ipBroker + ":" + puertoBroker;

        // Se establecen las propiedades:
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, idOperador + "-" + java.util.UUID.randomUUID());
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
     * Método que lee el fichero de los servicios a solicitar y se encarga del procesamiento y envío
     * de cada solicitud. Cada línea del archivo tendrá el código de la WS que se quiera activar.
     * @param idOperador
     * @param servicios
     */
    private static void ficheroSolicitudes(KafkaProducer<String, String> productor, String idOperador, String servicios) {
        try (BufferedReader reader = new BufferedReader(new FileReader(servicios))) {
            String linea;

            // Se lee cada solicitud de servicio del fichero:
            while ((linea = reader.readLine()) != null) {
                System.out.println("Leyendo solicitud de servicio: " + linea);

                // Se prepara la trama y la publicación en la cola. La trama será: "WATERING_REQUEST/codOperador/estacion/segundos". En cada línea del fichero aparecerá el formato "estacion/segundos"
                String mensaje = "WATERING_REQUEST/" + idOperador + "/" + linea;
                ProducerRecord<String, String> registro = new ProducerRecord<>(TOPIC_PRODUCTOR, idOperador, mensaje);

                // Se envía el mensaje de forma asíncrona, comprobando si ha fallado el envío o no:
                productor.send(registro, (metadata, e) -> {
                    if (e == null) {
                        System.out.println("Solicitud enviada con éxito a Central");
                    } else {
                        System.err.println("Ha fallado el envío de la solicitud");
                    }
                });

                // Se cierra la conexión:
                productor.flush(); // Envía todos los registros acumulados en el búfer
                Thread.sleep(4000); // Pausa de 4 segundos
            }

        } catch (IOException e) {
            System.err.println("Error durante la lectura del archivo: " + e.getMessage());

        } catch (InterruptedException e) {
            System.err.println("Error durante la espera: " + e.getMessage());
        }
    }



    /**
     * Función que permite crear una solicitud de riego dirigida hacia Central.
     * @param scanner
     * @param productor
     * @param idOperador
     */
    private static void crearSolicitud(Scanner scanner, KafkaProducer<String, String> productor, String idOperador) {
        String estacion;
        int segundos = 0;
        boolean tiempoValido = false;

        System.out.println("Estaciones disponibles actualmente: ");
        // AQUÍ VA EL LISTADO DE ESTACIONES DISPONIBLES

        System.out.println("Introduce el nombre de la estación deseada: ");
        estacion = scanner.nextLine();
        // FALTA VALIDAR EL NOMBRE DE LA ESTACIÓN INSERTADO

        while (!tiempoValido) {
            try {
                System.out.println("Introduce el tiempo que permanecerá activada (en segundos): ");
                segundos = Integer.parseInt(scanner.nextLine());

                if (segundos > 0) {
                    tiempoValido = true;
                } else {
                    System.out.println("Los segundos no pueden ser negativos");
                }
            } catch (NumberFormatException e) {
                System.out.println("Debes introducir un número entero válido");
            }
        }

        // Se prepara la trama a enviar y la publicación en la cola. La trama será: "WATERING_REQUEST/codOperador/estacion/segundos". En cada línea del fichero aparecerá el formato "estacion/segundos"
        String mensaje = "WATERING_REQUEST/" + idOperador + "/" + estacion +  "/" + segundos;
        ProducerRecord<String, String> registro = new ProducerRecord<>(TOPIC_PRODUCTOR, idOperador, mensaje);

        // Se envía el mensaje de forma asíncrona, comprobando si ha fallado el envío o no:
        productor.send(registro, (metadata, e) -> {
            if (e == null) {
                System.out.println("Solicitud enviada con éxito a Central");
            } else {
                System.err.println("Ha fallado el envío de la solicitud");
            }
        });

        productor.flush(); // Envía el registro
    }



    private static void mostrarEstaciones(KafkaProducer<String, String> productor, KafkaConsumer<String, String> consumidor, String idOperador) {
        // ---------------------------------------------------------
        // PETICIÓN DE ENVÍO DE LAS ESTACIONES ACTIVAS (PRODUCTOR):
        // ---------------------------------------------------------
        // Primero, solicitamos a Central el envío de todas las estaciones activas actualmente. Formato: "STATIONS_LIST/idOperador"
        String mensaje = "STATIONS_LIST/" + idOperador;
        ProducerRecord<String, String> registroP = new ProducerRecord<>(TOPIC_PRODUCTOR, idOperador, mensaje);

        // Se envía el mensaje de forma asíncrona, comprobando si ha fallado el envío o no:
        productor.send(registroP, (metadata, e) -> {
            if (e != null) {
                System.err.println("Ha fallado la comunicación con Central");
                return;
            }
        });

        productor.flush(); // Envía el registro

        System.out.println("Esperando respuesta de Central...");

        // --------------------------------------------------
        // RECEPCIÓN DE LAS ESTACIONES ACTIVAS (CONSUMIDOR):
        // --------------------------------------------------
        ConsumerRecords<String, String> registrosC = consumidor.poll(Duration.ofSeconds(5)); // Buscamos mensajes en un tiempo máximo de 5 segundos

        if (registrosC.isEmpty()) {
            System.out.println("No se ha recibido respuesta de central");
        } else {
            for (ConsumerRecord<String, String> registroC : registrosC) {
                // AQUÍ SE PROCESAN LAS ESTACIONES ENVIADAS POR CENTRAL
            }
        }
    }



    /**
     * Función principal.
     * @param args
     */
    public static void main(String[] args) {
        // --------------------------
        // VALIDACIÓN DE ARGUMENTOS:
        // --------------------------
        // Si los argumentos son incorrectos:
        if (args.length < 3 || args.length > 4) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas y el ID del operador");
            System.out.println("$./WM_FO ip_broker puerto_broker id_operador [fichero_servicios]");
            System.exit(1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String idOperador = args[2];

        // Si se ha introducido un fichero de solicitud de servicios:
        String servicios = null;
        if (args.length == 4) {
            servicios = args[3];
        }

        // -----------------------------
        // FUNCIONAMIENTO DEL PROGRAMA:
        // -----------------------------
        Scanner scanner = new Scanner(System.in); // Para la lectura de datos por consola
        KafkaProducer<String, String> productor = crearProductor(ipBroker, puertoBroker);
        KafkaConsumer<String, String> consumidor = crearConsumidor(ipBroker, puertoBroker, idOperador);

        // Se crea un hilo en paralelo que procesa los mensajes de Central:
        HiloConsumidor hilo = new HiloConsumidor(consumidor, idOperador);
        hilo.start();

        // Comienza la cuenta atrás para establecer la comunicación del consumidor. De esta forma, los FO no enviarán ninguna petición antes de que el consumidor esté listo
        try {
            if (!hilo.getListo().await(15, java.util.concurrent.TimeUnit.SECONDS)) {
                System.out.println("No se pudo conectar con el broker.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        clear();
        iniciarAplicacion(ipBroker, puertoBroker, idOperador); // Se inicia la aplicación

        boolean salir = false;
        while (!salir) {
            clear();
            int opcion = mostrarMenu(scanner); // Se muestra el menú y se guarda la opción escogida

            switch (opcion) {
                case 1:
                    if (servicios == null) {
                        System.out.println("No se ha indicado un fichero de servicios al iniciar la aplicación");
                    } else {
                        ficheroSolicitudes(productor, idOperador, servicios);
                    }
                    break;
                case 2:
                    crearSolicitud(scanner, productor, idOperador);
                    break;
                case 3:
                    mostrarEstaciones(productor, consumidor, idOperador);
                    break;
                case 4:
                    salir = true;
                    break;
            }
        }

        // Se cierran todas las conexiones y el scanner:
        productor.flush();
        productor.close();
        hilo.detener(); // El consumidor ya se cierra en el hilo.
        scanner.close();
    }
}

// COSAS QUE FALTA POR IMPLEMENTAR:
// 1. En crearSolicitud, falta incluir el listado de las estaciones de riego disponibles (195)
// 2. Comprobar la validez de la estación marcada en la misma función (199)
// 3. En mostrarEstaciones, falta procesar e imprimir por pantalla las estaciones (263)
// 4. Mostrar el estado de las estaciones de riego activadas
// 5. Hacer el main bien bien, es decir, hacer que funcione