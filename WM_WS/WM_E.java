import java.io.*;
import java.net.*;

public class WM_E {
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



    /**
     * Función principal.
     * @param args
     */
    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 4) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas y la IP y puerto del monitor");
			System.out.println("$./WM_WS_E ip_broker puerto_broker ip_monitor puerto_monitor");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String ipMonitor = args[2];
        String puertoMonitor = args[3];

        // ------------------------
        // CONEXIÓN CON EL MONITOR:
        // ------------------------
        try {
            iniciarEngine();
            
            Socket socketMonitor = new Socket(ipMonitor, Integer.parseInt(puertoMonitor));

            // Bucle infinito que espera la comprobación de estado de salud por parte del Monitor
            while (true) {
                String respuesta = leerSocket(socketMonitor);

                // No es necesario comprobar el mensaje, el Monitor siempre mandará al Engine un mensaje de STATUS:
                if (respuesta != null) {
                    escribirSocket(socketMonitor, "OK");
                }
            }
        } catch (Exception e) {
            System.out.println("Ha fallado la conexión con el Monitor");
            System.out.println("Error: " + e.getMessage());
        }
    }
}