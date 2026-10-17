package UA_SD.WM_WS;

public class WM_E {
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
    }
}