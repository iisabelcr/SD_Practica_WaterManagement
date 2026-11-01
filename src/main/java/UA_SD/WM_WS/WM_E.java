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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class WM_E {
    // Topics:
    private static final String TOPIC_PRODUCTOR = "engine_central";
    private static final String TOPIC_CONSUMIDOR = "central_engine";

    // Estados de la WS
    static volatile boolean averia = false; // Avería provocada desde el menú
    static volatile boolean bloqueada = false; // Fuera de servicio por orden de Central
    static volatile HiloRiego riegoActual = null; // Indica si hay un riego en curso o no (null si no lo hay)
    static volatile boolean monitorConectado = false; // Determina que el monitor sigue conectado al engine
    static volatile boolean centralConectada = true;
    static String idWS;
    static KafkaProducer<String, String> productor;
    static final BlockingQueue<String> eventos = new LinkedBlockingQueue<>(); // Cola de eventos (para que no se superpongan los mensajes)



    /**
     * Hilo que maneja la comunicación mediante sockets con el Monitor
     * HiloMonitor
     */
    static class HiloMonitor extends Thread {
        private final String ipMonitor;
        private final int puerto;

        public HiloMonitor(String ipMonitor, int puerto) {
            this.ipMonitor = ipMonitor;
            this.puerto = puerto;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                try (Socket socketMonitor = new Socket(ipMonitor, puerto)) {
                    System.out.println("Engine conectado con el Monitor");
                    socketMonitor.setSoTimeout(5000);
                    monitorConectado = true;

                    while (true) {
                        String respuesta = leerSocket(socketMonitor);

                        if (respuesta == null) {
                            monitorConectado = false;

                            // Si hay un riego en curso:
                            if (riegoActual != null) {
                                System.out.println("El monitor se ha caido, deteniendo el riego...");
                                detenerRiego();
                            }

                            break;
                        }

                        boolean centralAhora = !respuesta.endsWith("CENTRAL_KO");
                        if (centralAhora != centralConectada) {
                            centralConectada = centralAhora;
                            System.out.println(centralAhora ? "Conexión con WM_Central restablecida" : "Imposible conectar con WM_Central");
                        }

                        escribirSocket(socketMonitor, averia ? "KO" : "OK");
                    }

                } catch (Exception e) {
                    System.out.println("No se ha podido conectar con el Monitor: " + e.getMessage());
                }

                monitorConectado = false;
                System.out.println("Reintentando conexión con el Monitor en 3 segundos...");
                
                try {
                    Thread.sleep(3000);

                } catch (InterruptedException e) {}
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
            eventos.add("FIN");
        }
    }
    
    

    /**
     * Genera una pausa antes de volver al menú tras la ejecución de cualquier opción.
     * @param scanner
     */
    private static void pausar(Scanner scanner) {
        System.out.print("\nPulsa Enter para volver al menú... ");
        scanner.nextLine();
    }



    /**
     * Función que envía una petición de inicio de riego al Engine.
     * @param scanner
     */
    private static void solicitarRiegoManual(Scanner scanner) {
        System.out.print("Segundos de riego: ");
        int segundos;
        try {
            segundos = Integer.parseInt(scanner.nextLine().trim());
            if (segundos <= 0) throw new NumberFormatException();

        } catch (NumberFormatException e) {
            System.out.println("Debes introducir un número entero mayor que 0");
            return;
        }

        eventos.clear(); // Se vacía la cola
        enviarCentral("MANUAL_REQUEST/" + idWS + "/" + segundos);
        System.out.println("Solicitud enviada a Central. Esperando autorización...");

        try {
            // Se espera a la respuesta de Central, si autoriza o no:
            String resultado = eventos.poll(20, TimeUnit.SECONDS);

            if (resultado == null) {
                System.out.println("Central no ha respondido a tiempo");

            } else if (resultado.equals("INICIADO")) {
                System.out.println("Riego en marcha. Usa la opción 2 para finalizarlo o la 3 para simular un KO.");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
            else if (!monitorConectado) motivo = "El monitor no está conectado";
            else if (bloqueada) motivo = "La estación está fuera de servicio";
            else if (riegoActual != null) motivo = "La estación ya está regando";
        }

        // Si hay algún motivo por el que no se pueda iniciar el riego, se rechaza
        if (motivo != null) {
            System.out.println("Riego rechazado: " + motivo);
            enviarCentral("WATERING_RESPONSE/" + idWS + "/NOK/" + motivo);
            eventos.add("RECHAZADO");
            return;
        }

        enviarCentral("WATERING_RESPONSE/" + idWS + "/OK");
        System.out.println("Riego autorizado. Duración: " + segundos + " s");

        // Se inicia la secuencia de riego:
        riegoActual = new HiloRiego(segundos);
        riegoActual.start();
        eventos.add("INICIADO");
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

                    int posicion = operacion.equals("WATERING_REQUEST_ANSWER") ? 2 : 1; // Dependiendo de la operacion, el idWS se encontrará en una posición u otra
                    if (info.length <= posicion || !info[posicion].equalsIgnoreCase(idWS)) continue; // Si el mensaje no es para esta WS, se descarta

                    System.out.println("Orden recibida desde Central: " + registro.value());

                    // Si la trama de petición de riego es correcta:
                    if (operacion.equals("WATERING_REQUEST_ANSWER") && info.length >= 4) {
                        iniciarRiego(info[3]); // WATERING_REQUEST_ANSWER/operario/estacion/segundos

                    // Si Central bloquea la WS:
                    } else if (operacion.equals("BLOQUEAR")) {
                        bloqueada = true;
                        detenerRiego();
                        System.out.println("Estación fuera de servicio por orden de Central");

                    // Si Central la activa:
                    } else if (operacion.equals("ACTIVAR")) {
                        bloqueada = false;
                        System.out.println("Estación activada por orden de Central");
                    
                    // Si central no acepta la petición de riego manual
                    } else if (operacion.equals("MANUAL_DENIED")) {
                        System.out.println("Central ha denegado el riego manual: " + (info.length >= 3 ? info[2] : "motivo desconocido"));
                        eventos.add("RECHAZADO");
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
     * Función que muestra y procesa el menú.
     * @return opción escogida
     */
    private static int mostrarMenu(Scanner scanner) {
        System.out.println("+--------------------------------------+");
        System.out.println("|       WATERING STATION: ENGINE       |");
        System.out.println("+--------------------------------------+\n");

        System.out.println("ID: " + idWS);
        System.out.println("----------------------------------------\n");
        System.out.println("1. Solicitar riego manual");
        System.out.println("2. Finalizar el riego en curso");
        System.out.println("3. " + (averia ? "Reparar la avería" : "Provocar una avería"));
        System.out.println("4. Ver estado");
        System.out.println("5. Salir\n");

        int opcion = -1;
        boolean correcto = false;

        // Se seguirá pidiendo la opción hasta que se inserte un número correcto
        while (!correcto) {
            try {
                System.out.print("Selecciona una opción: ");
                opcion = Integer.parseInt(scanner.nextLine());

                if (opcion >= 1 && opcion <= 5) {
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
     * Procedimiento que muestra mensajes de iniciación del programa.
     */
    private static void iniciarEngine() {
        try {
            System.out.println("Iniciando el Engine de la Estación de riego...");
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

        clear();
        iniciarEngine();

        // Se inicia el hilo consumidor (para las comunicaciones Central-Engine)
        HiloConsumidorKafka hiloKafka = new HiloConsumidorKafka(ipBroker, puertoBroker);
        hiloKafka.setDaemon(true);
        hiloKafka.start();

        int puerto;
        try {
            puerto = Integer.parseInt(puertoMonitor);
        } catch (NumberFormatException e) {
            System.err.println("El puerto del Monitor debe ser un número");
            System.exit(1);
            return;
        }

        // Se inicia el hilo que mantiene la comunicación con el Monitor mediante sockets
        HiloMonitor hiloMonitor = new HiloMonitor(ipMonitor, puerto);
        hiloMonitor.start();

        // Se inicia el scanner
        Scanner scanner = new Scanner(System.in);

        try {
            boolean salir = false;

            while (!salir) {
                clear();
                int opcion = mostrarMenu(scanner);

                switch (opcion) {
                    case 1:
                        if (!monitorConectado) {
                            System.out.println("No se puede iniciar un riego manual: el Monitor no está conectado al Engine");
                        } else if (!centralConectada) {
                            System.out.println("Imposible conectar con WM_Central");
                        } else {
                            solicitarRiegoManual(scanner);
                        }

                        pausar(scanner);
                        break;

                    case 2:
                        if (riegoActual == null) {
                            System.out.println("No hay ningún riego en curso");

                        } else {
                            detenerRiego();
                        }

                        pausar(scanner);
                        break;

                    case 3:
                        averia = !averia;
                        if (averia) detenerRiego();
                        System.out.println(averia ? "Estación AVERIADA" : "Estación reparada");
                        
                        pausar(scanner);
                        break;

                    case 4:
                        System.out.println("Avería: " + averia + " | Fuera de servicio: " + bloqueada + " | Regando: " + (riegoActual != null) + " | Monitor conectado: " + monitorConectado + " | Central conectada: " + centralConectada);
                        pausar(scanner);
                        break;

                    case 5:
                        salir = true;
                        break;
                }
            }

        } catch (NoSuchElementException e) {
            // Sin teclado (contenedor sin -it): la estación sigue funcionando sin menú
            System.out.println("Sin teclado disponible: menú desactivado");
            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {}
        }

        System.out.println("Cerrando estación...");
        scanner.close();
        productor.close();
        System.exit(0);
    }
}