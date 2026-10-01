public class WM_FO {
    public static void main(String[] args) {
        // Si los argumentos son incorrectos:
        if (args.length < 3) {
            System.out.println("Debe indicar la IP y puerto del Broker y el ID del operador");
			System.out.println("$./WN_FO ip_broker puerto_broker id_operador");
			System.exit (1);
        }

        // Guardamos la información:
        String ipBroker = args[0];
        String puertoBroker = args[1];
        String idOperador = args[2];

        

    }
}