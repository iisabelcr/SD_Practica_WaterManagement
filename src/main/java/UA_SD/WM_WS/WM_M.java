package UA_SD.WM_WS;

import java.io.*;
import java.net.*;
public class WM_M {
    // Atributos de la WS (compartidos por todos los hilos):
    private static String idWS;
    private static String ubicacion;
    private static String ipCentral;
    private static int puertoCentral;
    private static volatile boolean averia = false;

    // Variables para la conexión con Central:
    private static Socket socketCentral;
    private static BufferedReader entradaCentral;
    private static PrintWriter salidaCentral;
    private static volatile boolean centralConectada = false;

    //Tramas para la comuncicación a través de los sockets:
    private static String tramaRegistro()     { return "REGISTER/" + idWS + "/" + ubicacion; }
    private static String tramaAveria()       { return "LEAK/" + idWS; }
    private static String tramaRecuperacion() { return "LEAK_RESOLVED/" + idWS; }
    private static String tramaEstado()       { return "STATUS/" + idWS; }

    /**
     * Clase interna que permite la comunicación entre el Monitor y el Engine de la WS de forma asíncrona
     * HiloServidorEngine
     */
    static class HiloServidorEngine extends Thread {
        private int puertoServidor;

        public HiloServidorEngine(int puerto) {
            this.puertoServidor = puerto;
        }

        public void run() {
            try {
                    ServerSocket servidor = new ServerSocket(puertoServidor);
                    System.out.println("Monitor a la escucha del Engine en el puerto local " + puertoServidor + "...");

                    while (true) {
                        Socket socketEngine = servidor.accept(); // El Monitor espera indefinidamente hasta que se conecte el Engine
                        System.out.println("Conexión establecida con el Engine");
                        
                        comprobarSalud(socketEngine); // Iniciamos la comprobación periódica del estado de salud
                        
                        // Se cierra el socket (y se procesa la excepción, en caso de que la haya)
                        try {
                            socketEngine.close();
                        } catch (IOException e) { }

                        System.out.println("Engine desconectado. Restableciendo conexión...");
                    }
            
            } catch (Exception e) {
                System.out.println("Fallo crítico en el Servidor del Monitor");
                System.out.println("Error: " + e.getMessage());
                System.exit(1);
            }
        }

        private void actualizarAveria(boolean nuevaAveria) {
            if (averia == nuevaAveria) {
                return; // No se repite una notificación del mismo estado
            }

            averia = nuevaAveria;

            if (nuevaAveria) {
                notificarCentral(tramaAveria());
                System.out.println("Se ha producido una avería en la WS. Esperando recuperación...");

            } else {
                notificarCentral(tramaRecuperacion());
                System.out.println("La WS se ha recuperado de la avería");
            }
        }

        private void comprobarSalud(Socket socketEngine) {
            try {
                socketEngine.setSoTimeout(2000); // Espera un máximo de dos segundos (aunque el Engine va a mandarle los mensajes cada segundo)
                int timeouts = 0; // Ayuda a que no se alerte de una avería al producirse un atraso anormal del mensaje de comprobación. Cuando valga más de uno, se notificará la avería

                // Manda información al Engine infinitamente:
                while (true) {
                    escribirSocket(socketEngine, tramaEstado()); // Manda un mensaje de comprobación de estado de salud
                    String respuesta = leerSocket(socketEngine);
                    
                    // Si el Engine no responde, se sale del bucle para volver a conectarse con él
                    if (respuesta == null) {
                        actualizarAveria(true);
                        break; 
                    }

                    // El mensaje de la comprobación de salud puede ser 'OK' o 'KO'
                    switch (respuesta) {
                        case "OK":
                            timeouts = 0;
                            actualizarAveria(false);
                            break;

                        case "KO":
                            timeouts = 0;
                            actualizarAveria(true);
                            break;

                        case "TIMEOUT": // En caso de que no responda pero la conexión siga abierta
                            timeouts++;
                            if (timeouts >= 2) { // No se tolera más de 1 timeout
                                actualizarAveria(true);
                            }
                            break;

                        default:
                            System.err.println("Respuesta de salud desconocida: " + respuesta);
                            break;
                    }

                    Thread.sleep(1000); // Se comprueba cada segundo
                }

            } catch (Exception e) {
                System.out.println("Se ha perdido la conexión con el Engine. Reconectando...");
                System.out.println("Error: " + e.getMessage());
                actualizarAveria(true);

            }
        }

        private void notificarCentral(String mensaje) {
            if (!enviarCentral(mensaje)) {
                System.out.println("Sin conexión con Central: el estado se sincronizará al reconectar");
            }
        }
    }



    /**
     * Clase interna que permite la comunicación entre el Monitor y Central.
     * HiloCentral
     */
    static class HiloCentral extends Thread {
        @Override
        public void run() {
            while (true) {
                if (conectarCentral()) {
                    if (averia) {
                        enviarCentral(tramaAveria()); // Si hay avería, hay que notificar a Central
                    }

                    escucharCentral(); // Mientras Central siga conectada, el hilo se quedará parado en esta función
                    desconectarCentral();
                    System.out.println("Se ha perdido la conexión con Central");
                }

                System.out.println("Restableciendo conexión con Central en 5 segundos...");
                try {
                    Thread.sleep(5000);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }



    /**
     * Función que permite la conexión con el socket de Central
     * @return booleano que indica el éxito de la conexión
     */
    private static boolean conectarCentral() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(ipCentral, puertoCentral), 5000);

            // Se crean los canales y se guardan. De esta forma es más fácil enviar y leer mensajes
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            out.println(tramaRegistro());

            socket.setSoTimeout(5000); // Tiempo máximo para la respuesta del registro
            String respuesta = in.readLine();
            socket.setSoTimeout(0); // Después, espera sin límite

            if (respuesta == null || !respuesta.trim().startsWith("OK")) {
                System.err.println("Central no ha aceptado el registro de la WS: " + respuesta);
                System.exit(1); // Sin registrarse, se aborta el programa
            }

            synchronized (WM_M.class) {
                socketCentral = socket;
                entradaCentral = in;
                salidaCentral = out;
                centralConectada = true;
            }

            System.out.println("WS registrada en Central (" + respuesta.trim() + ")");
            
            return true;

        } catch (Exception e) {
            System.out.println("No se ha podido conectar con Central");
            System.out.println("Error: " + e.getMessage());

            return false;
        }
    }



    /**
     * Envía a Central la trama pasada por parámetro,
     * @param trama
     * @return booleano que determina si se ha enviado la trama con éxito o no
     */
    private static synchronized boolean enviarCentral(String trama) {
        if (!centralConectada || salidaCentral == null) {
            return false;
        }

        salidaCentral.println(trama);
        if (salidaCentral.checkError()) { // En caso de error, se desconecta la conexión con Central
            desconectarCentral();

            return false;
        }

        return true;
    }



    /**
     * Escucha el socket de central para recibir los mensajes
     */
    private static void escucharCentral() {
        try {
            String linea;
            while ((linea = entradaCentral.readLine()) != null) {
                if (!linea.trim().startsWith("OK")) {
                    System.out.println("[Central] " + linea.trim());
                }
            }
        } catch (IOException e) {} // Si lanza una excepción, deja de escuchar
    }



    /**
     * Descanecta la conexión con Central
     */
    private static synchronized void desconectarCentral() {
        centralConectada = false;
        salidaCentral = null;
        try {
            if (socketCentral != null) socketCentral.close();
        } catch (IOException e) {}
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
     */
    private static void iniciarMonitor() {
        try {
            System.out.println("Iniciando el monitor...");
            Thread.sleep(2000);
            System.out.println("Estableciendo conexión con Central...");
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
        
        } catch (SocketTimeoutException e) {
            return "TIMEOUT";

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
			System.out.println("Ha fallado la conexión con el Engine");
            System.out.println("Error: " + e.getMessage());
		}
	}
    


    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 5) {
            System.out.println("Debe indicar el puerto del servidor que ofrece al engine, la IP y puerto de la central y el ID y ubicación de la WS");
			System.out.println("$./WM_WS_M puerto_servidor ip_central puerto_central id_ws ubicacion");
			System.exit(1);
        }

        // Guardamos la información:
        int puertoServidor = Integer.parseInt(args[0]);
        ipCentral = args[1];
        puertoCentral = Integer.parseInt(args[2]);
        idWS = args[3];
        ubicacion = args[4];

        iniciarMonitor();

        // Conexión con Central:
        HiloCentral hiloCentral = new HiloCentral();
        hiloCentral.start();

        // Conexión con el Engine:
        HiloServidorEngine hiloEngine = new HiloServidorEngine(puertoServidor);
        hiloEngine.start();
    }
}

