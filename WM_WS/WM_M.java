import java.io.*;
import java.net.*;
public class WM_M {
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
            System.err.println("Fallo al iniciar la aplicación");
        }
    }



    /**
     * Función que lee la información procedente del socket.
     * @param socket
     * @param datos
     * @return datos
     */
    private static String leerSocket (Socket socket) {
		try {
			InputStream aux = socket.getInputStream();
			DataInputStream flujo = new DataInputStream(aux);
			return flujo.readUTF();

		} catch (Exception e) {
			System.out.println("Ha fallado la conexión con Central");
            return null;
		}
	}



	/**
     * Método que escribe la información dentro del socket.
     * @param socket
     * @param datos
     */
	private static void escribirSocket (Socket socket, String datos) {
		try {
			OutputStream aux = socket.getOutputStream();
			DataOutputStream flujo = new DataOutputStream(aux);
			flujo.writeUTF(datos);

		} catch (Exception e) {
			System.out.println("Ha fallado la conexión con Central");
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
        try {
            // Se muestran los mensajes de inicio del monitor
            iniciarMonitor();

            Socket socketCentral = new Socket(ipCentral, Integer.parseInt(puertoCentral));

            // Se crea la trama que se enviará a Central para el registro de la estación:
            String datos = "REGISTRO/" + idWS;
            escribirSocket(socketCentral, datos);

            // Se lee la respuesta por parte de Central:
            String respuesta = leerSocket(socketCentral);

            socketCentral.close(); // Se cierra el socket
        } catch (Exception e) {
            // BLAAAAAAAAAA
        }
    }
}