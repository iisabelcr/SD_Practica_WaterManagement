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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                // Cierre normal, no es un error
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
     * Genera una pausa antes de volver al menú tras la ejecución de cualquier opción.
     * @param scanner
     */
    private static void pausar(Scanner scanner) {
        System.out.println("\nPulsa Enter para volver al menú...");
        scanner.nextLine();
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
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000); // Para agilizar el envío de los metadatos
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
     * Función que espera en la cola hasta que llegue un evento de un tipo en concreto y lo devuelve. Devuelve null si se pasa del tiempo pasado por parametro (en segundos).
     * @param hilo
     * @param idOperacion es el tipo de evento que se espera recibir
     * @param estacion solo acepta los eventos de esa estación
     * @param timeout
     */
    private static String[] esperarEvento(HiloConsumidor hilo, String idOperacion, String idEstacion, long timeout) {
        long limiteTiempo = System.currentTimeMillis() + timeout * 1000;

        try {
            while (true) {
                long restante = limiteTiempo - System.currentTimeMillis();
                if (restante <= 0) return null; 

                String[] evento = hilo.getEventos().poll(restante, TimeUnit.MILLISECONDS);
                if (evento == null) return null; // Se acabó el tiempo

                if (idOperacion != null && !evento[0].equals(idOperacion)) continue;
                if (idOperacion == null && evento[0].equals("STATIONS_LIST")) continue; // Lista antigua, se ignora
                if (idEstacion != null && !evento[1].equals(idEstacion)) continue;

                return evento;
            }
        } catch (InterruptedException e) {
            System.err.println("Fallo en el hilo consumidor: " + e.getMessage());
            Thread.currentThread().interrupt();
            return null;
        }
    }



    /**
     * Se encarga de realizar la petición de riego a Central y comprobar si hay respuesta o no. 
     * @param productor
     * @param hilo
     * @param idOperador
     * @param estacion
     * @param segundos
     */
    private static void solicitarRiego(KafkaProducer<String, String> productor, HiloConsumidor hilo, String idOperador, String idEstacion, int segundos) {
        hilo.getEventos().clear(); // Se descartan los eventos de peticiones anteriores

        String mensaje = "WATERING_REQUEST/" + idOperador + "/" + idEstacion + "/" + segundos;

        try {
            // Se publica el mensaje en la cola. Si no se ha recibido respuesta, lanzará una excepción y no se volverá a enviar:
            productor.send(new ProducerRecord<>(TOPIC_PRODUCTOR, idOperador, mensaje)).get(5, TimeUnit.SECONDS);

        } catch (Exception e) {
            System.err.println("No se ha podido enviar la solicitud al broker");
            return;
        }

        System.out.println("Solicitud enviada. Esperando a Central...");

        // Espera al evento lo que dure el riego más un margen de 20 segundos:
        String[] evento = esperarEvento(hilo, null, idEstacion, segundos + 20);

        if (evento == null) {
            System.out.println("Central no ha respondido a tiempo");

        } else if (evento[0].equals("WATERING_END")) {
            System.out.println("Resumen -> duración: " + evento[2] + " s | volumen total: " + evento[3] + " L");
        }
    }



    /**
     * Método que lee el fichero de los servicios a solicitar y se encarga del procesamiento y envío
     * de cada solicitud. Cada línea del archivo tendrá el código de la WS que se quiera activar.
     * @param productor
     * @param hilo
     * @param idOperador
     * @param servicios
     */
    private static void ficheroSolicitudes(KafkaProducer<String, String> productor, HiloConsumidor hilo, String idOperador, String servicios) {
        try (BufferedReader reader = new BufferedReader(new FileReader(servicios))) {
            String linea;

            // Se lee cada solicitud de servicio del fichero:
            while ((linea = reader.readLine()) != null) {
                if (linea.isBlank()) continue; // Si la linea está vacía se omite

                String[] info = linea.trim().split("/");

                if (info.length != 2) {
                    System.out.println("Línea con formato incorrecto, se omite: " + linea);
                    continue;
                }

                System.out.println("Leyendo solicitud de servicio: " + linea);
                solicitarRiego(productor, hilo, idOperador, info[0].trim(), Integer.parseInt(info[1]));
                Thread.sleep(4000); // Se espera 4 segundos antes de leer la siguiente petición 
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
     * @param hilo
     * @param idOperador
     */
    private static void crearSolicitud(Scanner scanner, KafkaProducer<String, String> productor, HiloConsumidor hilo, String idOperador) {
        System.out.println("Consultando estaciones disponibles...");
        List<String[]> estaciones = pedirEstaciones(productor, hilo, idOperador);

        if (estaciones == null) {
            System.out.println("No se ha recibido respuesta de Central. No se puede solicitar el riego.");
            return;
        }

        // Si no hay estaciones encendidas:
        if (estaciones.isEmpty()) {
            System.out.println("No hay estaciones disponibles ahora mismo.");
            return;
        }

        imprimirEstaciones(estaciones);

        // Se pide la estación hasta que sea una de la lista (o se cancela):
        String estacion = null;
        while (estacion == null) {
            System.out.println("Introduce el ID de la estación deseada (escribe 'q' para cancelar): ");
            String entrada = scanner.nextLine().trim();

            if (entrada.equalsIgnoreCase("q")) {
                return;
            }

            for (String[] est : estaciones) {
                if (est[0].equalsIgnoreCase(entrada)) {
                    estacion = est[0]; // Se usa el ID tal y como lo envía Central
                    break;
                }
            }
            
            if (estacion == null) {
                System.out.println("Esa estación no está entre las disponibles.");
            }
        }

        // Se piden los segundos hasta que sean un entero mayor que 0:
        int segundos = 0;

        while (segundos <= 0) {
            try {
                System.out.println("Introduce el tiempo que permanecerá activada (en segundos): ");
                segundos = Integer.parseInt(scanner.nextLine().trim());

                if (segundos <= 0) {
                    System.out.println("Los segundos deben ser mayores que 0");
                }

            } catch (NumberFormatException e) {
                System.out.println("Debes introducir un número entero válido");
            }
        }

        // Finalmente, se realiza la solicitud
        solicitarRiego(productor, hilo, idOperador, estacion, segundos); 
    }



    /**
     * Envía una solicitud a Central para que le mande todas las estaciones que están disponibles. 
     * @param productor
     * @param hilo
     * @param idOperador
     * @return estaciones
     */
    private static List<String[]> pedirEstaciones(KafkaProducer<String, String> productor, HiloConsumidor hilo, String idOperador) {
        hilo.getEventos().clear(); // Se descartan los eventos de peticiones anteriores

        try {
            // Se publica el mensaje en la cola. Si no se ha recibido respuesta, lanzará una excepción y no se volverá a enviar:
            productor.send(new ProducerRecord<>(TOPIC_PRODUCTOR, idOperador, "STATIONS_LIST/" + idOperador)).get(5, TimeUnit.SECONDS);

        } catch (Exception e) {
            System.err.println("No se ha podido comunicar con Central");
            return null;
        }

        String[] evento = esperarEvento(hilo, "STATIONS_LIST", null, 10);
        if (evento == null) return null; // Central no ha respondido

        List<String[]> estaciones = new ArrayList<>(); // Lista que almacena la infomación de cada estación

        for (int i = 1; i < evento.length; i++) {
            if (evento[i].equals("NONE")) break;
            estaciones.add(evento[i].split(":"));
        }

        return estaciones;
    }



    /**
     * Función que ordena e imprime cada estación de la lista pasada por referencia.
     * @param lista
     */
    private static void imprimirEstaciones(List<String[]> estaciones) {
        System.out.printf("%-10s | %s%n", "ID", "UBICACIÓN");
        for (String[] est : estaciones) {
            System.out.printf("%-10s | %s%n", est[0], est[1]);
        }
    }



    /**
     * Muestra por pantalla las estaciones disponibles.
     * @param productor
     * @param hilo
     * @param idOperador
     * @return lista de estaciones
     */
    private static void mostrarEstaciones(KafkaProducer<String, String> productor, HiloConsumidor hilo, String idOperador) {
        List<String[]> estaciones = pedirEstaciones(productor, hilo, idOperador); // Se piden las estaciones

        if (estaciones == null) {
            System.out.println("No se ha recibido respuesta de Central");

        } else if (estaciones.isEmpty()) {
            System.out.println("No hay estaciones disponibles ahora mismo");

        } else {
            imprimirEstaciones(estaciones); // Se imprimen las estaciones
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
        // Si no se ha podido conectar con el broker, se saldrá del programa
        boolean conectado = false;
        try {
            conectado = hilo.getListo().await(15, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!conectado) {
            System.out.println("No se ha podido conectar con el broker.");
            hilo.detener();
            productor.close(Duration.ofSeconds(1));
            System.exit(1);
        }

        clear();
        iniciarAplicacion(ipBroker, puertoBroker, idOperador); // Se inicia la aplicación

        boolean salir = false;
        while (!salir) {
            // Comprobamos que el hiloConsumidor sigue vivo para que no fallen las comunicaciones ni salten timeouts
            if (!hilo.isAlive()) {
                System.out.println("Ha finalizado la conexión con el broker inesperadamente");
                System.exit(1);
            }

            clear();
            int opcion = mostrarMenu(scanner); // Se muestra el menú y se guarda la opción escogida

            switch (opcion) {
                case 1:
                    if (servicios == null) {
                        System.out.println("No se ha indicado un fichero de servicios al iniciar la aplicación");
                    } else {
                        ficheroSolicitudes(productor, hilo, idOperador, servicios);
                    }
                    pausar(scanner);
                    break;
                case 2:
                    crearSolicitud(scanner, productor, hilo, idOperador);
                    pausar(scanner);
                    break;
                case 3:
                    mostrarEstaciones(productor, hilo, idOperador);
                    pausar(scanner);
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
        try {
            hilo.join(2000); // Se espera 2 segundes a que termine la detención del hilo. De esta forma, el programa se asegurará de que el consumidor termina antes de que finalice el programa (porque el hilo es un daemon)
        } catch (InterruptedException e) {
            System.out.println("Error al finalizar el hilo consumidor: " + e.getMessage());
        }
        scanner.close();
    }
}