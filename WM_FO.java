public class WM_FO {
    /**
     * Método que lee el fichero de los servicios a solicitar y se encarga del procesamiento y envío
     * de cada solicitud. Cada línea del archivo tendrá el código de la WS que se quiera activar.
     */
    private void lectorServicios(String servicios) {
        try (BufferedReader reader = new BufferedReader(new FileReader(servicios))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
                System.out.println("Leyendo petición: " + linea);
            }
        } catch (IOException e) {
            System.err.println("Error al leer el fichero: " + e.getMessage());
        }
    }


    /**
     * Función principal.
     */
    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 3 || args.length > 4) {
            System.out.println("Debe indicar la IP y puerto del Broker del gestor de colas y el ID del operador");
			System.out.println("$./WM_FO ip_broker puerto_broker id_operador [fichero_servicios]");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String idOperador = args[2];

        // Si se ha introducido un fichero de solicitud de servicios:
        if (args.length == 4) {
            String servicios = args[3];
        }

        System.out.println("+-----------------------------+");
        System.out.println("|       FIELD OPERATORS       |");
        System.out.println("+-----------------------------+\n");

        System.out.println("Iniciando la aplicación...");
        System.out.println("ID del operador: " + idOperador);
        System.out.println("Conectando al Broker del gestor de colas en: " + ipBroker + ":" + puertoBroker);

    }
}