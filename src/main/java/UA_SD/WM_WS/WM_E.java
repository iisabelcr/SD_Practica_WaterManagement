package UA_SD.WM_WS;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;


public class WM_E {
    // Topics:
    private static final String TOPIC_PRODUCTOR = "engine_central";
    private static final String TOPIC_CONSUMIDOR = "central_engine";

    // Estados de la WS
    static volatile boolean averia = false; // Avería provocada desde el menú
    static volatile boolean bloqueada = false; // Fuera de servicio por orden de Central
    static volatile HiloRiego riegoActual = null; // Indica si hay un riego en curso o no (null si no lo hay)
    static String idWS;
    static KafkaProducer<String, String> productor;


    
    /**
     * Clase interna que permite gestionar la lógica de provocar averías en un hilo distinto al principal
     * HiloKO
     */
    static class HiloKO extends Thread {
        @Override
        public void run() {
            Scanner scanner = new Scanner(System.in);
            try {
                while (true) {
                    // Menú interactivo:
                    System.out.println("\n--- ESTACIÓN " + idWS + " ---");
                    System.out.println("1. Solicitar riego manual");
                    System.out.println("2. Finalizar el riego en curso");
                    System.out.println("3. " + (averia ? "Reparar la avería" : "Provocar una avería"));
                    System.out.println("4. Ver estado");

                    switch (scanner.nextLine().trim()) {
                        case "1":
                            System.out.println("Segundos de riego: ");
                            try {
                                int seg = Integer.parseInt(scanner.nextLine().trim());
                                if (seg <= 0) throw new NumberFormatException();

                                // Se solicita a Central el inicio de riego manual:
                                enviarCentral("MANUAL_REQUEST/" + idWS + "/" + seg);
                                System.out.println("Solicitud enviada a Central. Esperando autorización...");

                            } catch (NumberFormatException e) {
                                System.out.println("Debes introducir un número entero mayor que 0");
                            }

                            break;

                        case "2":
                            if (riegoActual == null) {
                                System.out.println("No hay ningún riego en curso");

                            } else {
                                detenerRiego();
                            }

                            break;

                        case "3":
                            averia = !averia;
                            if (averia) {
                                detenerRiego(); // Una avería corta el riego inmediatamente
                            }  
                            System.out.println(averia ? "Estación AVERIADA" : "Estación reparada"); // Dependiendo de si está averiada o no, se muestra un mensaje u otro
                            break;

                        case "4":
                            System.out.println("Avería: " + averia + " | Fuera de servicio: " + bloqueada + " | Regando: " + (riegoActual != null));
                            break;

                        default:
                            System.out.println("Opción no válida");
                    }
                }
            } catch (NoSuchElementException e) {
                // Sin teclado (contenedor sin -it): la estación sigue funcionando sin menú
                System.out.println("Sin teclado disponible: menú desactivado");
            }
        }
    }



    /**
     * Hilo que simula un riego: cada segundo envía caudal y volumen a Central y termina por tiempo o cuando se le ordena detenerse.
     * HiloRiego
     */
    static class HiloRiego extends Thread {
        private final int segundos;
        private volatile boolean detenido = false;

        public HiloRiego(int segundos) {
            this.segundos = segundos;
        }

        public void detener() {
            detenido = true;
            interrupt(); // El riego termina inmediatamente
        }

        @Override
        public void run() {
            double volumen = 0.0;
            int transcurridos = 0;

            while (!detenido && transcurridos < segundos) {
                try {
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    break;
                }

                if (detenido) {
                    break;
                }

                transcurridos++; // Segundos que va durando el riego

                double caudal = 8 + Math.random() * 4; // Caudal simulado: 8-12 L/min
                volumen += caudal / 60.0;

                enviarCentral("WATERING_DATA/" + idWS + "/" + ajustarValor(caudal) + "/" + ajustarValor(volumen));
                System.out.println("Regando -> caudal: " + ajustarValor(caudal) + " L/min | volumen: " + ajustarValor(volumen) + " L | " + transcurridos + "/" + segundos + " s");
            }

            // Se libera antes de avisar, para que Central pueda pedir otro riego enseguida
            synchronized (WM_E.class) {
                if (riegoActual == this) {
                    riegoActual = null;
                }
            }

            enviarCentral("WATERING_END/" + idWS + "/" + transcurridos + "/" + ajustarValor(volumen));
            System.out.println("Riego finalizado (" + transcurridos + " s, " + ajustarValor(volumen) + " L)");
        }
    }
    
    

    /**
     * Función que inicia el riego de la estación
     * @param segundosTexto
     */
    private static synchronized void iniciarRiego(String segundosTexto) {
        String motivo = null;
        int segundos = 0;

        try {
            segundos = Integer.parseInt(segundosTexto.trim());
            if (segundos <= 0) motivo = "Tiempo no válido";
            
        } catch (NumberFormatException e) {
            motivo = "Tiempo no válido";
        }

        if (motivo == null) {
            if (averia) motivo = "La estación está averiada";
            else if (bloqueada) motivo = "La estación está fuera de servicio";
            else if (riegoActual != null) motivo = "La estación ya está regando";
        }

        // Si hay algún motivo por el que no se pueda iniciar el riego, se rechaza
        if (motivo != null) {
            System.out.println("Riego rechazado: " + motivo);
            enviarCentral("WATERING_RESPONSE/" + idWS + "/NOK/" + motivo);
            return;
        }

        enviarCentral("WATERING_RESPONSE/" + idWS + "/OK");
        System.out.println("Riego autorizado. Duración: " + segundos + " s");

        // Se inicia la secuencia de riego:
        riegoActual = new HiloRiego(segundos);
        riegoActual.start();
    }



    /**
     * Función que detiene el riego, independientemente de si es forzado o ha terminado por tiempo
     */
    private static synchronized void detenerRiego() {
        if (riegoActual != null) {
            riegoActual.detener(); // El hilo ya avisa a Central al terminar
        }
    }



    /**
     * Se encarga de producir el envío de información (cualquier mensaje) a Central
     * @param mensaje
     */
    private static void enviarCentral(String mensaje) {
        productor.send(new ProducerRecord<>(TOPIC_PRODUCTOR, idWS, mensaje), (metadata, e) -> {
            if (e != null) {
                System.err.println("No se pudo enviar a Central el mensaje: " + mensaje);
            }
        });
    }



    /**
     * Ajusta el valor para que tenga dos decimales y se muestre con un punto decimal
     * @param valor
     * @return valor modificado
     */
    private static String ajustarValor(double valor) {
        return String.format(java.util.Locale.US, "%.2f", valor); 
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
            KafkaConsumer<String, String> consumidor = crearConsumidor(ipBroker, puertoBroker, idWS);
            
            while (true) {
                // Recibimos el mensaje por parte de Central (o varios mensajes):
                ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofSeconds(1));

                // Procesamos cada mensaje:
                for (ConsumerRecord<String, String> registro : registros) {
                    String[] info = registro.value().split("/");
                    String operacion = info[0];

                    int posicion = operacion.equals("WATERING_REQUEST") ? 2 : 1; // Dependiendo de la operacion, el idWS se encontrará en una posición u otra
                    if (info.length <= posicion || !info[posicion].equalsIgnoreCase(idWS)) continue; // Si el mensaje no es para esta WS, se descarta

                    System.out.println("Orden recibida desde Central: " + registro.value());

                    // Si la trama de petición de riego es correcta:
                    if (operacion.equals("WATERING_REQUEST") && info.length >= 4) {
                        iniciarRiego(info[3]); // WATERING_REQUEST/operario/estacion/segundos

                    // Si Central bloquea la WS:
                    } else if (operacion.equals("BLOQUEAR")) {
                        bloqueada = true;
                        detenerRiego();
                        System.out.println("Estación fuera de servicio por orden de Central");

                    // Si Central la activa:
                    } else if (operacion.equals("ACTIVAR")) {
                        bloqueada = false;
                        System.out.println("Estación activada por orden de Central");
                    }
                }
            }
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
			System.out.println("Ha fallado la conexión con el Monitor");
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
			System.out.println("Ha fallado la conexión con el Monitor");
            System.out.println("Error: " + e.getMessage());
		}
	}



    /**
     * Función que crea el productor para las comunicaciones Engine-Central con los topics de Kafka.
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
     * Función que crea el consumidor las comunicaciones Central-Engine con los topics de Kafka.
     * @param ipBroker
     * @param puertoBroker
     * @param idOperador
     * @return consumidor
     */
    private static KafkaConsumer<String, String> crearConsumidor(String ipBroker, String puertoBroker, String idWS) {
        String bootstrapServers = ipBroker + ":" + puertoBroker;

        // Se establecen las propiedades:
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-engine-" + idWS + "-" + java.util.UUID.randomUUID());
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
        if (args.length < 5) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas, la IP y puerto del monitor y el ID de la WS");
			System.out.println("$./WM_WS_E ip_broker puerto_broker ip_monitor puerto_monitor id_ws");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String ipMonitor = args[2];
        String puertoMonitor = args[3];
        idWS = args[4];

        productor = crearProductor(ipBroker, puertoBroker); // Se crea el productor

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
        
        iniciarEngine();

        int puerto;
        try {
            puerto = Integer.parseInt(puertoMonitor);

        } catch (NumberFormatException e) { // Se lanza una excepción si el puerto no es un número
            System.err.println("El puerto del Monitor debe ser un número");
            System.exit(1);
            return;
        }

        // Conecta con el Monitor. Si falla, vuelve a intentarlo
        while (true) {
            try (Socket socketMonitor = new Socket(ipMonitor, puerto)) {
                System.out.println("Conectado con el Monitor");

                while (true) {
                    String respuesta = leerSocket(socketMonitor);
                    if (respuesta == null) break; // El Monitor se ha caído: se vuelve a conectar

                    // El Monitor siempre manda "STATUS/id", por lo que no hace falta comprobar la respuesta:
                    escribirSocket(socketMonitor, averia ? "KO" : "OK");
                }
            } catch (Exception e) {
                System.out.println("No se ha podido conectar con el Monitor: " + e.getMessage());
            }

            System.out.println("Reintentando conexión con el Monitor en 3 segundos...");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {}
        }
    }
}