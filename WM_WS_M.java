public class WM_WS_M {
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
    }
}