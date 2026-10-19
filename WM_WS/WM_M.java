import java.io.*;
import java.net.*;
public class WM_M {
    /**
     * Clase interna que permite la comunicación entre el Monitor y el Engine de la WS de forma asíncrona
     * HiloServidorEngine
     */
    static class HiloServidorEngine extends Thread {
        private int puertoServidor;
        private Socket socketCentral;
        private String idWS;

        public HiloServidorEngine(int puerto, Socket socketCentral, String idWS) {
            this.puertoServidor = puerto;
            this.socketCentral = socketCentral;
            this.idWS = idWS;
        }

        public void run() {
            try {
                    ServerSocket servidor = new ServerSocket(puertoServidor);
                    System.out.println("Monitor a la escucha del Engine en el puerto local " + puertoServidor + "...");

                    Socket socketEngine = servidor.accept(); // El Monitor espera indefinidamente hasta que se conecte el Engine
                    System.out.println("Conexión establecida con el Engine");

                    comprobarSalud(socketEngine); // Iniciamos la comprobación periódica del estado de salud
            
            } catch (Exception e) {
                System.out.println("Ha fallado la conexión con el Engine");
                System.out.println("Error: " + e.getMessage());
            }
        }

        private void comprobarSalud(Socket socketEngine) {
            try {
                // Manda información al Engine infinitamente:
                while (true) {
                    escribirSocket(socketEngine, "STATUS"); // Manda un mensaje de comprobación de estado de salud

                    String respuesta = leerSocket(socketEngine);

                    if (respuesta.equals("KO")) {
                        String mensaje = "AVERIA/" + idWS;
                        escribirSocket(socketCentral, mensaje);

                        while (leerSocket(socketEngine) == "KO") {
                            wait();
                        }
                    }

                    Thread.sleep(1000); // Provoca el envío cada segundo
                }
            } catch (Exception e) {
                System.out.println("Se ha perdido la conexión con el Engine");
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
        if (args.length < 4) {
            System.out.println("Debe indicar el puerto del servidor que ofrece al engine, la IP y puerto de la central y el ID de la WS");
			System.out.println("$./WM_WS_M puerto_servidor ip_central puerto_central id_ws");
			System.exit (1);
        }

        // Guardamos la información:
        String puertoServidor = args[0];
        String ipCentral = args[1];
        String puertoCentral = args[2];
        String idWS = args[3];

        // ---------------------------------
        // CONEXIÓN Y REGISTRO CON CENTRAL:
        // ---------------------------------
        Socket socketCentral = new Socket();
        try {
            // Se muestran los mensajes de inicio del Monitor
            iniciarMonitor();

            socketCentral = new Socket(ipCentral, Integer.parseInt(puertoCentral));

            // Se crea la trama que se enviará a Central para el registro de la estación. Formato: "REGISTRO/idWs"
            String datos = "REGISTRO/" + idWS;
            escribirSocket(socketCentral, datos);

            // Se lee la respuesta por parte de Central:
            String respuesta = leerSocket(socketCentral);
            // AQUÍ SE PROCESA LA RESPUESTA

            socketCentral.close(); // Se cierra el socket
        } catch (Exception e) {
            System.out.println("Ha fallado la conexión con Central");
            System.out.println("Error: " + e.getMessage());
        }

        // ------------------------
        // CONEXIÓN CON EL ENGINE:
        // ------------------------
        // Creamos el hilo servidor entre el Monitor y el Engine y lo activamos:
        HiloServidorEngine servidor = new HiloServidorEngine(Integer.parseInt(puertoServidor), socketCentral, idWS);
        servidor.start();

    }
}


// COSAS QUE HACER:
//1. Gestionar la lógica de las averías dentro del hilo servidor