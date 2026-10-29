package UA_SD.WM_WS;

import java.io.*;
import java.net.*;
public class WM_M {
    /**
     * Clase interna que permite la comunicación entre el Monitor y el Engine de la WS de forma asíncrona
     * HiloServidorEngine
     */
    static class HiloServidorEngine extends Thread {
        private int puertoServidor;
        private String ipCentral;
        private int puertoCentral;
        private String idWS;
        private volatile boolean averia = false;

        public HiloServidorEngine(int puerto, String ipCentral, int puertoCentral, String idWS) {
            this.puertoServidor = puerto;
            this.ipCentral = ipCentral;
            this.puertoCentral = puertoCentral;
            this.idWS = idWS;
        }

        public void run() {
            try {
                    ServerSocket servidor = new ServerSocket(puertoServidor);
                    System.out.println("Monitor a la escucha del Engine en el puerto local " + puertoServidor + "...");

                    while (true) {
                        Socket socketEngine = servidor.accept(); // El Monitor espera indefinidamente hasta que se conecte el Engine
                        System.out.println("Conexión establecida con el Engine");
                        
                        comprobarSalud(socketEngine); // Iniciamos la comprobación periódica del estado de salud
                    }
            
            } catch (Exception e) {
                System.out.println("Fallo crítico en el Servidor del Monitor");
                System.out.println("Error: " + e.getMessage());
            }
        }

        private void actualizarAveria(boolean nuevaAveria) {
            if (averia == nuevaAveria) {
                return; // No se repite una notificación del mismo estado
            }

            averia = nuevaAveria;

            if (nuevaAveria) {
                notificarCentral("AVERIA/" + idWS);
                System.out.println("Se ha producido una avería en la WS. Esperando recuperación...");

            } else {
                notificarCentral("RECUPERACION/" + idWS);
                System.out.println("La WS se ha recuperado de la avería");
            }
        }

        private void comprobarSalud(Socket socketEngine) {
            try {
                // Manda información al Engine infinitamente:
                while (true) {
                    escribirSocket(socketEngine, "STATUS"); // Manda un mensaje de comprobación de estado de salud
                    String respuesta = leerSocket(socketEngine);
                    socketEngine.setSoTimeout(3000); // Espera un máximo de tres segundos (aunque el Engine va a mandarle los mensajes cada segundo)
                    
                    // Si el Engine no responde, se sale del bucle para volver a conectarse con él
                    if (respuesta == null) {
                        actualizarAveria(true);
                        break; 
                    }

                    // El mensaje de la comprobación de salud puede ser 'OK' o 'KO'
                    switch (respuesta) {
                        case "OK":
                            actualizarAveria(false);
                            break;

                        case "KO":
                            actualizarAveria(true);
                            break;

                        default:
                            System.err.println("Respuesta de salud desconocida: " + respuesta);
                            break;
                    }
                }

            } catch (Exception e) {
                System.out.println("Se ha perdido la conexión con el Engine. Reconectando...");
                System.out.println("Error: " + e.getMessage());

            }
        }

        private void notificarCentral(String mensaje) {
            try {
                Socket socketCentral = new Socket(ipCentral, puertoCentral);
                escribirSocket(socketCentral, mensaje);

            } catch (Exception e) {
                System.out.println("Fallo al intentar notificar el estado a Central");
                System.out.println("Error: " + e.getMessage());
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