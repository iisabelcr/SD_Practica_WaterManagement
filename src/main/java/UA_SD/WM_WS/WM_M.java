package UA_SD.WM_WS;

import java.io.*;
import java.net.*;

import UA_SD.WM_WS.WM_M.HiloServidorEngine;
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
     * @param datos
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
    


    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 5) {
            System.out.println("Debe indicar el puerto del servidor que ofrece al engine, la IP y puerto de la central y el ID y ubicación de la WS");
			System.out.println("$./WM_WS_M puerto_servidor ip_central puerto_central id_ws ubicacion");
			System.exit(1);
        }

        // Guardamos la información:
        String puertoServidor = args[0];
        String ipCentral = args[1];
        String puertoCentral = args[2];
        String idWS = args[3];
        String ubicacion = args[4];

        // ---------------------------------
        // CONEXIÓN Y REGISTRO CON CENTRAL:
        // ---------------------------------
        Socket socketCentral = new Socket();
        try {
            // Se muestran los mensajes de inicio del Monitor
            iniciarMonitor();

            socketCentral = new Socket(ipCentral, Integer.parseInt(puertoCentral));

            // Se crea la trama que se enviará a Central para el registro de la estación. Formato: "REGISTRO/idWs"
            String datos = "REGISTER/" + idWS + "/" + ubicacion;
            escribirSocket(socketCentral, datos);

            // Se lee la respuesta por parte de Central:
            String respuesta = leerSocket(socketCentral);

            if (respuesta == null || !respuesta.startsWith("OK")) {
                System.err.println("Central no ha aceptado el registro de la WS");
                System.exit(1); // Si no se produce el registro, se finaliza el programa
            }

            socketCentral.close();
        } catch (Exception e) {
            System.out.println("Ha fallado la conexión con Central");
            System.out.println("Error: " + e.getMessage());
        }

        // ------------------------
        // CONEXIÓN CON EL ENGINE:
        // ------------------------
        // Creamos el hilo servidor entre el Monitor y el Engine y lo activamos:
        HiloServidorEngine servidor = new HiloServidorEngine(Integer.parseInt(puertoServidor), ipCentral, Integer.parseInt(puertoCentral), idWS);
        servidor.start();
    }
}


// COSAS QUE HACER:
// Leer la respuesta de registro por parte de central
// Quiero hacer que los mensajes de los sockets se preparen en funciones, para poder cambiarlos más fácilmente