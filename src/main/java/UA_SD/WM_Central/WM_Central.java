package UA_SD.WM_Central;

import UA_SD.MODELAJEDATOS.Estacion;
import UA_SD.MODELAJEDATOS.Operario;

import java.io.IOException;
import java.util.*;
import java.net.Socket;
import java.net.ServerSocket;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

public class WM_Central {
    private List<Estacion> estaciones;
    private List<Operario> operarios;
    private HashMap<String, ClientHandler> mapeoestaciones;
    private KafkaProducer<String, String> producer;

    public WM_Central(){
        this.estaciones=new ArrayList<>();
        this.operarios=new ArrayList<>();
        this.mapeoestaciones = new HashMap<>();
    }

    public WM_Central(List<Estacion> estaciones, List<Operario> operarios){
        this.estaciones=estaciones;
        this.operarios=operarios;
        this.mapeoestaciones = new HashMap<>();
    }

    public List<Operario> getOperarios() {
        return operarios;
    }

    public List<Estacion> getaestaciones() {
        return estaciones;
    }

    public void anadirestacion(Estacion estacion){
        if(estacion!=null) {
            this.estaciones.add(estacion);
            DatabaseManager.insertarEstacion(estacion);
        }
    }

    public void setOperarios(List<Operario> operarios) {
        if (operarios != null) {
            this.operarios = operarios;
        }
    }

    public boolean actualEstadoEstacion(String id,String nueEstado){
        boolean aux=false;
        for(int i=0;i<estaciones.size() && !aux ;i++){
            if(estaciones.get(i).getId().equalsIgnoreCase(id)){
                Estacion est = estaciones.get(i);
                est.setEstado(nueEstado);
                DatabaseManager.actualizarEstacion(est);
                aux=true;
            }
        }
        return aux;
    }

    public void setestaciones(List<Estacion> estaciones) {
        if (estaciones != null) {
            this.estaciones = estaciones;
        }
    }

    public void menu(){
        Thread hiloConsoleMenu = new Thread(new Runnable() {
            @Override
            public void run() {
                Scanner scanner = new Scanner(System.in);
                while(true){
                    System.out.println("========================================");
                    System.out.println("========================================");
                    System.out.println("Opción 1: Iniciar un riego manual");
                    System.out.println("Opción 2: Bloquear estación=>Fuera de Servicio");
                    System.out.println("Opción 3: Activar / Desbloquear estación");
                    System.out.println("Opción 4: Ver el estado de todas las estaciones");
                    System.out.println("========================================");
                    String opcion = scanner.nextLine();

                    if(opcion.equalsIgnoreCase("1")){
                        System.out.println("Dame el id de la estacion");
                        String idEstacion=scanner.nextLine();
                        System.out.println("Dame lo que va a durar regando");
                        String segundos=scanner.nextLine();
                        boolean aux=WM_Central.this.estacionokyexiste(idEstacion);
                        if(aux){
                            ClientHandler handler = WM_Central.this.getMapeoestaciones().get(idEstacion);
                            if (handler != null) {
                                String mensaje = "SOLICITUD_RIEGO/" + "CENTRAL" + "/" + idEstacion + "/" + segundos;
                                handler.enviarTrama(mensaje,"central_engine");
                            } else {
                                System.out.println("La estación no está conectada por Socket");
                            }
                        }
                        else {
                            System.out.println("No se puede iniciar el riego: la estación no existe o no está disponible.");
                        }

                    }
                    else if(opcion.equalsIgnoreCase("2")){
                        System.out.println("Dame el id de la estacion");
                        String idEstacion=scanner.nextLine();
                        boolean aux=false;
                        boolean encontrar=false;
                        for(int i=0;i<WM_Central.this.estaciones.size() && !encontrar;i++){
                            Estacion est = WM_Central.this.estaciones.get(i);
                            if(idEstacion.equalsIgnoreCase(est.getId())){
                                if(!est.getEstado().equalsIgnoreCase("LEAK") && !est.getEstado().equalsIgnoreCase("DISCONNECTED")) {
                                    encontrar=true;
                                    if(est.getEstado().equalsIgnoreCase("WATERING")){
                                        aux=true;
                                    }
                                    est.setEstado("OUT_OF_SERVICE");
                                    est.setCaudalactual(0.0);
                                    String idOperarioAnt=est.getOperarioId();
                                    est.setOperarioId(null);
                                    DatabaseManager.actualizarEstacion(est);
                                    String mensajeKafkaEngine = "BLOQUEAR/" + idEstacion;
                                    WM_Central.this.enviarmensajekafka(mensajeKafkaEngine, "central_engine");
                                    if(aux && idOperarioAnt!=null && !idOperarioAnt.equalsIgnoreCase("CENTRAL")){
                                        String mensajeKafkaFO = "FIN_RIEGO/" + idEstacion + "/0/0";
                                        WM_Central.this.enviarmensajekafka(mensajeKafkaFO, "central_fo", idOperarioAnt);
                                    }
                                }
                            }
                        }
                        if(!encontrar) {
                            System.out.println("Error: La estación con ID " + idEstacion + " no existe.");
                        }
                    }
                    else if(opcion.equalsIgnoreCase("3")){
                        System.out.println("Dame el id de la estacion a activar:");
                        String idEstacion = scanner.nextLine();
                        boolean encontrar=false;
                        for(int i=0;i<WM_Central.this.estaciones.size() && !encontrar;i++){
                            Estacion est = WM_Central.this.estaciones.get(i);
                            if(idEstacion.equalsIgnoreCase(est.getId())){
                                if(est.getEstado().equalsIgnoreCase("OUT_OF_SERVICE")){
                                    est.setEstado("AVAILABLE");
                                    est.setCaudalactual(0.0);
                                    est.setOperarioId(null);
                                    DatabaseManager.actualizarEstacion(est);
                                    String mensajeKafkaEngine = "ACTIVAR/" + idEstacion;
                                    WM_Central.this.enviarmensajekafka(mensajeKafkaEngine, "central_engine");
                                    encontrar=true;
                                    System.out.println("Estación " + idEstacion + " activada correctamente.");
                                }
                            }
                        }
                        if(!encontrar) {
                            System.out.println("Error: La estación con ID " + idEstacion + " no existe o no está en OUT_OF_SERVICE.");
                        }
                    }
                    else if(opcion.equalsIgnoreCase("4")){
                        System.out.println("\n---------------------------------------------------------------------------------------------------");
                        System.out.printf("%-10s | %-20s | %-25s | %-10s | %-10s | %-10s%n", "ID", "UBICACIÓN", "ESTADO", "CAUDAL", "VOLUMEN", "OPERARIO");
                        System.out.println("---------------------------------------------------------------------------------------------------");

                        List<Estacion> lista = DatabaseManager.leerTodasEstaciones();
                        for (Estacion est : lista) {
                            String estadoColoreado = est.getEstado();

                            switch (est.getEstado().toUpperCase()) {
                                case "AVAILABLE":
                                    estadoColoreado = "\u001B[32mAVAILABLE\u001B[0m"; // Verde
                                    break;
                                case "WATERING":
                                    estadoColoreado = "\u001B[32;5mWATERING\u001B[0m"; // Verde Parpadeante
                                    break;
                                case "OUT_OF_SERVICE":
                                    estadoColoreado = "\u001B[33mOUT OF SERVICE\u001B[0m"; // Naranja
                                    break;
                                case "LEAK":
                                case "AVERIA":
                                case "FUGA":
                                    estadoColoreado = "\u001B[31mLEAK / AVERIA\u001B[0m"; // Rojo
                                    break;
                                case "DISCONNECTED":
                                default:
                                    estadoColoreado = "\u001B[90mDISCONNECTED\u001B[0m"; // Gris
                                    break;
                            }

                            String operarioStr = (est.getOperarioId() != null) ? est.getOperarioId() : "-";

                            System.out.printf("%-10s | %-20s | %-34s | %-10.2f | %-10.2f | %-10s%n",
                                    est.getId(),
                                    est.getUbicacion(),
                                    estadoColoreado,
                                    est.getCaudalactual(),
                                    est.getVolumenacum(),
                                    operarioStr);
                        }
                        System.out.println("---------------------------------------------------------------------------------------------------\n");
                    }
                }
            }
        });
        hiloConsoleMenu.start();
    }
    public static void main(String[] args){
        String puerto;
        String ipKafka;
        String puertoKafka;

        try {
            // Validamos que se pasen los 3 parámetros mínimos exigidos
            if (args.length < 3) {
                System.out.println("Indicame el puerto_socket ip_broker_kafka puerto_broker_kafka");
                System.exit(1);// Detiene la ejecución limpia y controlada si faltan argumentos
            }

            puerto = args[0];
            ipKafka = args[1];
            puertoKafka = args[2];

            DatabaseManager.initDatabase();
            WM_Central central = new WM_Central();
            List<Estacion> guardadasestaciones = DatabaseManager.leerTodasEstaciones();

            for(int i = 0; i < guardadasestaciones.size(); i++){
                guardadasestaciones.get(i).setEstado("DISCONNECTED");
            }

            central.setestaciones(guardadasestaciones);

            // Usamos las variables en lugar de poner "localhost" y "9092" a piñón fijo
            central.incioconsumidorkafkaoperario(ipKafka, puertoKafka, "fo_central");
            central.incioconsumidorkafkaengine(ipKafka, puertoKafka, "engine_central");
            central.inicioproductokafka(ipKafka, puertoKafka);

            central.menu();

            try(ServerSocket servidorsocket = new ServerSocket(Integer.parseInt(puerto))){
                for(;;) {
                    Socket skCliente = servidorsocket.accept();
                    System.out.println("Sirviendo cliente...");

                    ClientHandler handler = new ClientHandler(skCliente, central);
                    Thread t = new Thread(handler);
                    t.start();
                }
            } catch(IOException e){
                System.err.println("No se ha podido establecer un socket en el puerto " + puerto);
            }
        }
        catch(Exception e) {
            System.err.println("Error en los argumentos de inicio: " + e.getMessage());
        }
    }

    public HashMap<String, ClientHandler> getMapeoestaciones() {
        return mapeoestaciones;
    }

    public void inicioproductokafka(String ipBroker, String puertoBroker){
        String bootstrapServers = ipBroker + ":" + puertoBroker;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer= new KafkaProducer<>(props);
    }

    public void enviarmensajekafka(String mensaje, String topic){
        if(this.producer!=null){
            this.producer.send(new ProducerRecord<>(topic, mensaje));
        }
    }

    public void enviarmensajekafka(String mensaje, String topic,String key){
        if(this.producer!=null){
            this.producer.send(new ProducerRecord<>(topic, key ,mensaje));
        }
    }

    public void incioconsumidorkafkaengine(String ipBroker, String puertoBroker,String topic){
        Thread hiloKafka = new Thread(new Runnable() {
            @Override
            public void run() {
                String bootstrapServers = ipBroker + ":" + puertoBroker;

                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-consumidores-engine");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));
                while(true){
                    try {
                        while (true) {
                            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                            for (ConsumerRecord<String, String> record : records) {
                                String textoCompleto = record.value();
                                String[] trama = textoCompleto.split("/");
                                if(trama.length>=2) {
                                    String mensaje = trama[0];
                                    String idEstacion = trama[1];
                                    Estacion est = DatabaseManager.obtenerEstacion(idEstacion);
                                    String idOperario = (est != null) ? est.getOperarioId() : null;

                                    if (trama[0].equalsIgnoreCase("WATERING_RESPONSE") && trama.length >= 3) {
                                        String estado = trama[2];
                                        if (estado.equalsIgnoreCase("OK")) {
                                            System.out.println("Riego iniciado con éxito en la estación " + idEstacion);
                                            String mensajeKafka = "WATERING_RESPONSE/" + idEstacion + "/OK";
                                            enviarmensajekafka(mensajeKafka, "central_fo", idOperario);
                                        } else {
                                            String mensajeKafka = "WATERING_RESPONSE/" + idEstacion + "/NOK";
                                            enviarmensajekafka(mensajeKafka, "central_fo", idOperario);
                                        }
                                    } else if (trama[0].equalsIgnoreCase("WATERING_DATA") && trama.length >= 4) {
                                        double caudal = Double.parseDouble(trama[2].replace(',', '.'));
                                        double volumen = Double.parseDouble(trama[3].replace(',', '.'));
                                        System.out.println("Mira funciona bien los datos de riego mira esto " + idEstacion + caudal + volumen);
                                        String mensajeKafka = "WATERING_DATA/" + idEstacion + "/" + caudal + "/" + volumen;
                                        enviarmensajekafka(mensajeKafka, "central_fo", idOperario);
                                    } else if (trama[0].equalsIgnoreCase("WATERING_END") && trama.length >= 4) {
                                        double tiempo = Double.parseDouble(trama[2].replace(',', '.'));
                                        double volumen = Double.parseDouble(trama[3].replace(',', '.'));
                                        System.out.println("OOOOOOOOOOO SE HA FINALIZADO" + idEstacion + tiempo + volumen);
                                        boolean encontrar=false;
                                        for (int i=0;i<WM_Central.this.estaciones.size() && !encontrar;i++) {
                                            if (WM_Central.this.estaciones.get(i).getId().equalsIgnoreCase(idEstacion)) {
                                                WM_Central.this.estaciones.get(i).setEstado("AVAILABLE");
                                                WM_Central.this.estaciones.get(i).setOperarioId(null);
                                                WM_Central.this.estaciones.get(i).setCaudalactual(0.0);
                                                DatabaseManager.actualizarEstacion(WM_Central.this.estaciones.get(i));
                                                encontrar=true;
                                            }
                                        }
                                        WM_Central.this.actualEstadoEstacion(idEstacion, "AVAILABLE");

                                        String mensajeKafka = "WATERING_END/" + idEstacion + "/" + tiempo + "/" + volumen;
                                        enviarmensajekafka(mensajeKafka, "central_fo", idOperario);
                                    }
                                    else {
                                        System.out.println("STATUS/ERROR/Operacion no valida");
                                    }
                                }
                                else{
                                    System.out.println("Fallo de numero de slipts en la trama");
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        System.err.println("Error en el poll " + e.getMessage());
                    }
                }
            }
        });
        hiloKafka.start();
    }

    public void incioconsumidorkafkaoperario(String ipBroker, String puertoBroker, String topic){
        Thread hiloKafka = new Thread(new Runnable() {
            @Override
            public void run() {
                String bootstrapServers = ipBroker + ":" + puertoBroker;

                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-consumidores-fo");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));
                while(true){
                    try {
                        while (true) {
                            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                            for (ConsumerRecord<String, String> record : records) {
                                String textoCompleto = record.value();
                                System.out.println(textoCompleto);
                                String[] trama = textoCompleto.split("/");

                                if (trama.length >= 2) {
                                    String idOperario = trama[0];
                                    String codOperacion = trama[1];

                                    // CASO 01: Petición de Riego
                                    if (codOperacion.equalsIgnoreCase("01") && trama.length >= 4) {
                                        String idEstacion = trama[2];
                                        String segundos = trama[3];

                                        boolean llimpia = asignaroperario(idOperario, idEstacion);
                                        if (llimpia) {
                                            ClientHandler handler = WM_Central.this.getMapeoestaciones().get(idEstacion);
                                            if (handler != null) {
                                                String mensaje = "WATERING_REQUEST/" + idOperario + "/" + idEstacion + "/" + segundos;
                                                handler.enviarTrama(mensaje, "central_engine");
                                            } else {
                                                System.out.println("La estación no está conectada por Socket");
                                            }
                                        }
                                    }
                                    // CASO 02: Petición de Listado de Estaciones Disponibles
                                    else if (codOperacion.equalsIgnoreCase("02")) {
                                        String listaEst = "STATIONS_LIST";
                                        for (int i = 0; i < WM_Central.this.getaestaciones().size(); i++) {
                                            if (WM_Central.this.getaestaciones().get(i).getEstado().equalsIgnoreCase("AVAILABLE")) {
                                                listaEst += "/" + WM_Central.this.getaestaciones().get(i).getId() + ":" + WM_Central.this.getaestaciones().get(i).getUbicacion();
                                            }
                                        }
                                        WM_Central.this.enviarmensajekafka(listaEst, "central_fo", idOperario);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        System.err.println("Error en el poll " + e.getMessage());
                    }
                }
            }
        });
        hiloKafka.start();
    }

    public void agregarmap(String idEstacion, ClientHandler handler){
        this.mapeoestaciones.put(idEstacion,handler);
    }

    public void eliminarmap(String idEstacion){
        this.mapeoestaciones.remove(idEstacion);
    }

    public boolean asignaroperario(String idOperario,String idEstacion){
        boolean aux=false;
        for(int i=0;i<this.estaciones.size() && !aux;i++){
            if(idEstacion.equalsIgnoreCase(estaciones.get(i).getId())){
                if(!(estaciones.get(i).getEstado().equalsIgnoreCase("LEAK")) && !(estaciones.get(i).getEstado().equalsIgnoreCase("OUT_OF_SERVICE"))){
                    Estacion est = estaciones.get(i);
                    est.setOperarioId(idOperario);
                    DatabaseManager.actualizarEstacion(est);
                    aux=true;
                }
                else{
                    System.out.println("La estación tiene una fuga, averia o está fuera de servicio");
                }
            }
        }
        if(!aux) {
            System.out.println("La estación no está registrada en el sistema");
        }
        return aux;
    }

    public boolean estacionokyexiste(String idEstacion){
        boolean aux=false;
        for(int i=0;i<this.estaciones.size() && !aux;i++){
            if(idEstacion.equalsIgnoreCase(estaciones.get(i).getId())){
                if(estaciones.get(i).getEstado().equalsIgnoreCase("AVAILABLE")){
                    estaciones.get(i).setEstado("WATERING");
                    estaciones.get(i).setOperarioId("CENTRAL");
                    DatabaseManager.actualizarEstacion(estaciones.get(i));
                    aux=true;
                }
            }
        }
        return aux;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }
    public synchronized void procesarDesconexionEstacion(String idEstacion) {
        boolean aux=false;
        if (idEstacion == null) return;
        for (int i=0;i<getaestaciones().size() && !aux;i++) {
            if (idEstacion.equalsIgnoreCase(getaestaciones().get(i).getId())) {
                String estadoAnt = getaestaciones().get(i).getEstado();
                String idOperarioAnt = getaestaciones().get(i).getOperarioId();

                getaestaciones().get(i).setEstado("DISCONNECTED");
                getaestaciones().get(i).setCaudalactual(0.0);
                getaestaciones().get(i).setOperarioId(null);


                DatabaseManager.actualizarEstacion(getaestaciones().get(i));

                // Hay que avisar al operario de que se desconecta la estacoion que estaba en ese momento regando
                if ("WATERING".equalsIgnoreCase(estadoAnt) && idOperarioAnt != null) {
                    System.out.println("La estacion con el " + idEstacion + " se ha desconectado durante un riego activo.");

                    // Enviar aviso de fin/interrupción de riego al operario asignado vía Kafka
                    if (!idOperarioAnt.equalsIgnoreCase("CENTRAL")) {
                        String mensajeKafkaFO = "WATERING_END/" + idEstacion + "/0/0";
                        enviarmensajekafka(mensajeKafkaFO, "central_fo", idOperarioAnt);
                    }
                }
                aux=true;
            }
        }
    }
}