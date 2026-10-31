package UA_SD.WM_Central;

import UA_SD.MODELAJEDATOS.Estacion;
import UA_SD.MODELAJEDATOS.Operario;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    //Se usan colecciones seguras para hilos porque las tocan el menú, los dos consumidores de Kafka y un hilo por cliente
    private List<Estacion> estaciones;
    private List<Operario> operarios;
    private Map<String, ClientHandler> mapeoestaciones;
    private volatile KafkaProducer<String, String> producer;
    //Este es el constructor de la clase central sin parametrizar,utilizo
    //el copyonwritearraylist porque para que los multiples hilos accedan,lean
    //y modifiquen las listas ya sea la de estaciones o operarios simultaneamente sin
    //que se cause ninguna corrupcion de dato.Antes utilizaba el arraylist pero cuando
    //estuvimos haciendo pruebas no funcionaba correctamente entonces preguntamos en su
    //momento a la ia y nos comento que esta seria una opcion concurrente.
    public WM_Central(){
        this.estaciones=new CopyOnWriteArrayList<>();
        this.operarios=new CopyOnWriteArrayList<>();
        this.mapeoestaciones = new ConcurrentHashMap<>();
    }
    //Este es el conctructor parametrizado
    public WM_Central(List<Estacion> estaciones, List<Operario> operarios){
        this.estaciones=new CopyOnWriteArrayList<>(estaciones);
        this.operarios=new CopyOnWriteArrayList<>(operarios);
        this.mapeoestaciones = new ConcurrentHashMap<>();
    }
    //El getoperarios y el setoperarios no lo utilizo podria quitarlos
    public List<Operario> getOperarios() {
        return operarios;
    }

    public List<Estacion> getaestaciones() {
        return estaciones;
    }

    public void setOperarios(List<Operario> operarios) {
        if (operarios != null) {
            this.operarios = new CopyOnWriteArrayList<>(operarios);
        }
    }

    public void setestaciones(List<Estacion> estaciones) {
        if (estaciones != null) {
            this.estaciones = new CopyOnWriteArrayList<>(estaciones);
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
                        String idEstacion = scanner.nextLine().trim();
                        System.out.println("Dame lo que va a durar regando");
                        String segundos = scanner.nextLine().trim();

                        // Pasamos el tercer parámetro "CENTRAL" y recibimos el String con el error (si lo hay)
                        String error = WM_Central.this.iniciarRiegoManual(idEstacion, segundos, "CENTRAL");

                        if(error == null){
                            System.out.println("Riego manual solicitado");
                        }
                        else {
                            System.out.println("No se puede iniciar el riego: " + error);
                        }

                    }
                    else if(opcion.equalsIgnoreCase("2")){
                        System.out.println("Dame el id de la estacion");
                        String idEstacion=scanner.nextLine().trim();
                        boolean noerror = WM_Central.this.bloquearEstacion(idEstacion);
                        if(noerror) {
                            System.out.println("Estación " + idEstacion + " bloqueada correctamente.");
                        }
                        else {
                            System.out.println("Error no se pudo bloquear correctamente");
                        }
                    }
                    else if(opcion.equalsIgnoreCase("3")){
                        System.out.println("Dame el id de la estacion a activar:");
                        String idEstacion = scanner.nextLine().trim();
                        boolean noerror = WM_Central.this.activarEstacion(idEstacion);
                        if(noerror) {
                            System.out.println("Estación " + idEstacion + " activada correctamente.");
                        }
                        else {
                            System.out.println("Error no se activado la estacion correctamente");
                        }
                    }
                    else if(opcion.equalsIgnoreCase("4")){
                        System.out.println("\n---------------------------------------------------------------------------------------------------");
                        System.out.println("\n---------------------------------------------------------------------------------------------------");
                        System.out.printf("%-10s | %-20s | %-25s | %-10s | %-10s | %-10s%n", "ID", "UBICACIÓN", "ESTADO", "CAUDAL", "VOLUMEN", "OPERARIO");
                        System.out.println("---------------------------------------------------------------------------------------------------");
                        System.out.println("\n---------------------------------------------------------------------------------------------------");

                        List<Estacion> lista = WM_Central.this.getaestaciones();
                        for (int i = 0; i < lista.size(); i++) {
                            Estacion est = lista.get(i);
                            String estadoColoreado = est.getEstado();
                            if (lista.get(i).getEstado().equalsIgnoreCase("AVAILABLE")) {
                                estadoColoreado = "\u001B[32mAVAILABLE\u001B[0m"; // Verde
                            }
                            else if (lista.get(i).getEstado().equalsIgnoreCase("WATERING")) {
                                estadoColoreado = "\u001B[32;5mWATERING\u001B[0m"; // Verde parpadeante
                            }
                            else if (lista.get(i).getEstado().equalsIgnoreCase("OUT_OF_SERVICE")) {
                                estadoColoreado = "\u001B[33mOUT OF SERVICE\u001B[0m"; // Naranja
                            }
                            else if (lista.get(i).getEstado().equalsIgnoreCase("LEAK")) {
                                estadoColoreado = "\u001B[31mLEAK / AVERIA\u001B[0m"; // Rojo
                            }
                            else if (lista.get(i).getEstado().equalsIgnoreCase("DISCONNECTED")) {
                                estadoColoreado = "\u001B[90mDISCONNECTED\u001B[0m"; // Gris
                            }
                            String operarioStr="-";
                            if(lista.get(i).getOperarioId() != null){
                                operarioStr=est.getOperarioId();
                            }
                            System.out.printf("%-10s | %-20s | %-34s | %-10.2f | %-10.2f | %-10s%n",
                                    lista.get(i).getId(),
                                    lista.get(i).getUbicacion(),
                                    estadoColoreado,
                                    lista.get(i).getCaudalactual(),
                                    lista.get(i).getVolumenacum(),
                                    operarioStr);
                        }
                        System.out.println("---------------------------------------------------------------------------------------------------\n");
                    }
                }
            }
        });
        hiloConsoleMenu.start();
    }
    public KafkaProducer<String, String> getProducer() {
        return producer;
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

            //Al arrancar ninguna estación está conectada. Se conserva OUT_OF_SERVICE (bloqueo manual)
            //y el cambio se guarda también en la BD para que memoria y BD queden iguales
            for(int i = 0; i < guardadasestaciones.size(); i++){
                Estacion e = guardadasestaciones.get(i);
                if(!e.getEstado().equalsIgnoreCase("OUT_OF_SERVICE")){
                    e.setEstado("DISCONNECTED");
                }
                e.setCaudalactual(0.0);
                e.setOperarioId(null);
                DatabaseManager.actualizarEstacion(e);
                System.out.printf("Estación -> ID: %s | Ubicación: %s | Estado: %s | Caudal: %.2f | Vol: %.2f%n",
                        e.getId(), e.getUbicacion(), e.getEstado(), e.getCaudalactual(), e.getVolumenacum());
            }

            central.setestaciones(guardadasestaciones);

            // El productor se tiene que crear antes para evtar errores de suscripcion
            central.inicioproductokafka(ipKafka, puertoKafka);
            central.incioconsumidorkafkaoperario(ipKafka, puertoKafka, "fo_central");
            central.incioconsumidorkafkaengine(ipKafka, puertoKafka, "engine_central");

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
            System.err.println("Error al iniciar Central: " + e.getMessage());
        }
    }
    //Esta funcion permite verficar que estaciones tiene atualmente un socket abierto y activo con la central.
    //Nos permite obtener una handler una conexion en especifico, que esta asociado a una estacion para
    //enviarle mensajes o tramas directas por TCP
    public Map<String, ClientHandler> getMapeoestaciones() {
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
    //Método para enviar un mensaje a un tópico de Kafka.Comprueba que el Producer esté inicializado y, si lo está,envía el mensaje al tópico indicado.
    public void enviarmensajekafka(String mensaje, String topic){
        if(this.producer!=null){
            this.producer.send(new ProducerRecord<>(topic, mensaje));
        }
    }
    //Método para enviar un mensaje a un tópico de Kafka utilizando una clave.Kafka utiliza la clave para determinar la partición a la que se envía
    //el mensaje, manteniendo el orden de los mensajes con la misma clave.
    public void enviarmensajekafka(String mensaje, String topic,String key){
        if(this.producer!=null){
            this.producer.send(new ProducerRecord<>(topic, key ,mensaje));
        }
    }

    /*
        Esta funcion es solo para comprobar de que el operario no es null y que no es la Central
        quien lo envia.Podria no haber hecho esta funcion y solo poner un if antes y ya llamar
        a la otra funcion que llama pero como lo hago varias veces pues lo puse a fuera.
     */
    private void enviarAFO(String operario, String mensaje){
        if(operario != null && !operario.equalsIgnoreCase("CENTRAL")){
            enviarmensajekafka(mensaje, "central_fo", operario);
        }
    }
    /*
    Por un lado, tenemos la siguiente función, que se encarga de configurar e iniciar
    el consumidor de Kafka para escuchar y recibir los mensajes del Engine. Por otro lado,
    tenemos la función de abajo, que se encarga de interpretar los mensajes recibidos,
    es decir, las tramas, y realizar las operaciones correspondientes sobre las estaciones.
    La primera función no se declara como synchronized porque únicamente se encarga
    de crear e iniciar un hilo que realiza la escucha continua de Kafka. Si se declarase
    como synchronized, el metodo mantendría bloqueado el acceso al objeto mientras se
    ejecutase su contenido.
    La segunda función sí se declara como synchronized porque modifica la información
    compartida de las estaciones. De esta forma, evitamos que varios hilos
    ejecuten simultáneamente operaciones sobre dicha información.
     */
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
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                            for (ConsumerRecord<String, String> record : records) {
                                String textoCompleto = record.value();
                                String[] trama = textoCompleto.split("/");
                                if (trama.length >= 2) {
                                    procesarMensajeEngine(trama);
                                } else {
                                    System.out.println("Fallo de numero de slipts en la trama");
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("[KAFKA ERROR] Error procesando registros: " + e.getMessage());
                        }
                        //Se quita el Thread.sleep(1000): el poll ya espera, y el sleep solo añadía latencia
                    }
                } catch (Exception e) {
                    System.err.println("Error en el poll " + e.getMessage());
                }
            }
        });
        hiloKafka.start();
    }
    /*
     Aquí se procesan las tramas recibidas del Engine. Estas nos indican si el
     inicio de un riego se ha realizado correctamente o si ha sido rechazado
     Además, se reciben los datos del riego y se informa cuando un riego ha finalizado.
     Cuando se recibe la confirmación de inicio, se informa al operario de si el riego
     ha comenzado correctamente o si ha sido rechazado.Si se reciben datos del riego,
     se actualizan en memoria y se envían al operario. Finalmente, cuando se recibe
     la trama de finalización, se actualizan los datos de la estación,
     se libera al operario y se le comunica que el riego ha terminado.
 */
    private synchronized void procesarMensajeEngine(String[] trama){
        String mensaje = trama[0];
        String idEstacion = trama[1];

        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null){
            System.out.println("Mensaje del Engine para una estación desconocida: " + idEstacion);
            return;
        }
        String idOperario = est.getOperarioId();

        try {
            if (mensaje.equalsIgnoreCase("WATERING_RESPONSE") && trama.length >= 3) {
                String estado = trama[2];
                if (estado.equalsIgnoreCase("OK")) {
                    System.out.println("Riego iniciado con éxito en la estación " + idEstacion);
                    enviarAFO(idOperario, "WATERING_RESPONSE/" + idEstacion + "/OK");
                } else {
                    String motivo= "Rechazado por la estación";
                    if (trama.length >= 4) {
                        motivo = trama[3];
                    }
                    enviarAFO(idOperario, "WATERING_RESPONSE/" + idEstacion + "/NOK/" + motivo);
                    //La estación rechazó el riego: se libera para que no se quede reservada para siempre
                    liberarOperario(idEstacion);
                }
            }
            else if (mensaje.equalsIgnoreCase("WATERING_DATA") && trama.length >= 4) {
                double caudal = Double.parseDouble(trama[2].replace(',', '.'));
                double volumen = Double.parseDouble(trama[3].replace(',', '.'));
                //Se guardan en memoria para que el menú los muestre. No se escribe en la BD en cada dato (se hace al acabar)
                est.setCaudalactual(caudal);
                est.setVolumenacum(volumen);
                enviarAFO(idOperario, "WATERING_DATA/" + idEstacion + "/" + caudal + "/" + volumen);
            }
            else if (mensaje.equalsIgnoreCase("WATERING_END") && trama.length >= 4) {
                double tiempo = Double.parseDouble(trama[2].replace(',', '.'));
                double volumen = Double.parseDouble(trama[3].replace(',', '.'));
                est.setVolumenacum(volumen);
                est.setCaudalactual(0.0);
                est.setOperarioId(null);
                //Solo vuelve a AVAILABLE si estaba regando: así no se pisa un OUT_OF_SERVICE o un LEAK
                if (est.getEstado().equalsIgnoreCase("WATERING")) {
                    est.setEstado("AVAILABLE");
                }
                actualizarEstacionEnSistema(est);
                enviarAFO(idOperario, "WATERING_END/" + idEstacion + "/" + tiempo + "/" + volumen);
            }
            else if (mensaje.equalsIgnoreCase("MANUAL_REQUEST") && trama.length >= 3) {
                String error = iniciarRiegoManual(idEstacion, trama[2], "MANUAL");
                if (error==null) {
                    enviarmensajekafka("MANUAL_DENIED/" + idEstacion + "/" + error, "central_engine");
                }
            }
            else {
                System.out.println("STATUS/ERROR/Operacion no valida");
            }
        } catch (NumberFormatException e) {
            System.err.println("Valores numéricos incorrectos en la trama del Engine");
        }
    }
    /*
        Por un lado, tenemos la siguiente función, que se encarga de configurar e iniciar
        el consumidor de Kafka para escuchar y recibir los mensajes del Operario.Y lo mismo
        que en el kafka del engine o se declara como synchronized porque únicamente se encarga
        de crear e iniciar un hilo que realiza la escucha continua de Kafka. Si se declarase
        como synchronized, el metodo mantendría bloqueado el acceso al objeto mientras se
        ejecutase su contenido.
     */
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
                                    String codOperacion = trama[0];
                                    String idOperario = trama[1];

                                    if (codOperacion.equalsIgnoreCase("WATERING_REQUEST") && trama.length >= 4) {
                                        String idEstacion = trama[2];
                                        String segundos = trama[3];

                                        boolean llimpia = asignaroperario(idOperario, idEstacion);
                                        ClientHandler handler = WM_Central.this.getMapeoestaciones().get(idEstacion);
                                        if (llimpia && handler != null) {
                                            String mensaje = "WATERING_REQUEST_ANSWER/" + idOperario + "/" + idEstacion + "/" + segundos;
                                            handler.enviarTrama(mensaje, "central_engine");
                                        } else {
                                            if (llimpia) {
                                                System.out.println("La estación no está conectada por Socket");
                                                liberarOperario(idEstacion);
                                            }
                                            //Se avisa al FO para que no se quede esperando al timeout
                                            enviarmensajekafka("WATERING_RESPONSE/" + idEstacion + "/NOK/Estacion no disponible", "central_fo", idOperario);
                                        }
                                    }
                                    else if (codOperacion.equalsIgnoreCase("SHOW_STATIONS")) {
                                        String listaEst = "STATIONS_LIST";
                                        boolean hay = false;
                                        for (int i = 0; i < WM_Central.this.getaestaciones().size(); i++) {
                                            if (WM_Central.this.getaestaciones().get(i).getEstado().equalsIgnoreCase("AVAILABLE")) {
                                                listaEst += "/" + WM_Central.this.getaestaciones().get(i).getId() + ":" + WM_Central.this.getaestaciones().get(i).getUbicacion();
                                                hay = true;
                                            }
                                        }
                                        if (!hay) {
                                            listaEst += "/NONE";
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

    /**
     * Solo borra la entrada si el handler del mapa es el que se pasa. Así, si el Monitor ya reconectó
     * con un handler nuevo, el viejo no lo borra.
     * @return true si se ha borrado
     */
    public boolean eliminarmap(String idEstacion, ClientHandler handler){
        return this.mapeoestaciones.remove(idEstacion, handler);
    }

    /**
     * Reserva la estación para un operario: solo se acepta si está AVAILABLE y pasa a WATERING al instante,
     * para que dos FO no puedan pedir la misma estación a la vez.
     */
    public synchronized boolean asignaroperario(String idOperario,String idEstacion){
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null){
            System.out.println("La estación no está registrada en el sistema");
            return false;
        }
        if(!est.getEstado().equalsIgnoreCase("AVAILABLE")){
            System.out.println("La estación " + idEstacion + " no está disponible (estado: " + est.getEstado() + ")");
            return false;
        }
        est.setEstado("WATERING");
        est.setOperarioId(idOperario);
        actualizarEstacionEnSistema(est);
        return true;
    }

    /**
     * Deshace una reserva que no llegó a empezar. No toca LEAK ni OUT_OF_SERVICE.
     */
    public synchronized void liberarOperario(String idEstacion){
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est != null && est.getEstado().equalsIgnoreCase("WATERING")){
            est.setEstado("AVAILABLE");
            est.setOperarioId(null);
            est.setCaudalactual(0.0);
            actualizarEstacionEnSistema(est);
        }
    }

    /* Mediante esta funcion comprobamos de que si se puede ejecutar el riego de forma manual
        primero debemos saber si estan en la memoria en la estacion y de que si la estacion
        esta conectada tambien de que si su estado es available,luego una comprobacion de los segundos que
        tiene que ser mayor que 0 y que sea un numero entero y ya si es asi en la ram pone que esta en estado
        de watering que lo envia la central porque es manual y envia una trama por el socket para decirle.
        CAMBIAR EL TOPICO
     */
    public synchronized String iniciarRiegoManual(String idEstacion, String segundos, String origen){
        String error = null;
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null){
            System.out.println("La estación no existe");
            error = "La estacion no existe";
        }
        else {
            ClientHandler handler = this.mapeoestaciones.get(idEstacion);
            if (handler == null) {
                System.out.println("La estación no está conectada");
                error = "La estacion no esta conectada";
            }
            else{
                if(!est.getEstado().equalsIgnoreCase("AVAILABLE")){
                    System.out.println("La estación no está disponible ya que su estado es  " + est.getEstado() + ")");
                    error = "La estacion no esta disponible (" + est.getEstado() + ")";
                }
                else{
                    int seg = 0;
                    boolean aux2 = false;

                    try{
                        seg = Integer.parseInt(segundos);
                        aux2 = true;
                    } catch (NumberFormatException e) {
                        System.out.println("Los segundos deben ser un número entero");
                        error = "Los segundos deben ser un numero entero";
                    }
                    if (aux2) {
                        if (seg <= 0) {
                            System.out.println("Los segundos deben ser mayores que 0");
                            error = "Los segundos deben ser mayores que 0";
                        } else {
                            est.setEstado("WATERING");
                            est.setOperarioId(origen);
                            actualizarEstacionEnSistema(est);

                            handler.enviarTrama("WATERING_REQUEST_ANSWER/" + origen + "/" + idEstacion + "/" + segundos, "central_engine");
                            // Si todo fue bien, 'error' permanece null
                        }
                    }
                }
            }
        }
        return error;
    }

    /*
     Mediante esta función comprobamos si se puede bloquear una estación.
     Primero buscamos la estación en la memoria y comprobamos que exista. Después comprobamos que no esté ya fuera de servicio (OUT_OF_SERVICE).
     Si se puede bloquear, guardamos si estaba regando (WATERING) y el ID del operario que estaba trabajando en ella.
     A continuación, cambiamos en la RAM el estado de la estación a OUT_OF_SERVICE, ponemos el caudal a 0 y eliminamos el ID del operario.
     Después actualizamos la información en el sistema para que los cambios también se reflejen en la base de datos.
     Luego enviamos mediante Kafka el mensaje BLOQUEAR al Engine para comunicarle que esa estación debe bloquearse.
     Si la estación estaba regando, también enviamos mediante Kafka al FO un mensaje de fin de riego para informar de que el operario ha terminado.
     Finalmente, devolvemos true si se ha podido bloquear la estación o false si no se ha podido realizar la operación.
     */
    public synchronized boolean bloquearEstacion(String idEstacion){
        boolean aux=false;
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null){
            System.out.println("La estación con ID " + idEstacion + " no existe.");
        }
        else{
            if(est.getEstado().equalsIgnoreCase("OUT_OF_SERVICE")) {
                System.out.println("La estación ya está fuera de servicio.");
            }
            else{
                boolean regaba = est.getEstado().equalsIgnoreCase("WATERING");
                String idOperarioAnt = est.getOperarioId();

                est.setEstado("OUT_OF_SERVICE");
                est.setCaudalactual(0.0);
                est.setOperarioId(null);
                actualizarEstacionEnSistema(est);
                aux=true;
                String mensajeKafkaEngine = "BLOQUEAR/" + idEstacion;
                enviarmensajekafka(mensajeKafkaEngine, "central_engine");
                if(regaba){
                    //Si habia algun operario regando(es decir si el estado de la esatcion era WATERING)
                    //se le manda atraves de kafka lo de fin de riego
                    enviarAFO(idOperarioAnt, "WATERING_END/" + idEstacion + "/0/0");
                }
            }
        }
        return aux;
    }

    /*
     * Mediante esta función comprobamos si se puede activar una estación. Primero buscamos la estación en la memoria y comprobamos que exista.
     * Después comprobamos que su estado sea OUT_OF_SERVICE, si se puede activar, comprobamos si la estación está conectada mediante
     * el mapeo de estaciones. Si está conectada, cambiamos su estado a VAILABLE y, si no está conectada, su estado será DISCONNECTED.
     * A continuación, ponemos el caudal a 0 y eliminamos el ID del operario.Después actualizamos la información en el sistema para que los cambios
     * también se reflejen en la base de datos. Luego enviamos mediante Kafka el mensaje ACTIVAR al Engine para comunicarle que esa estación debe activarse.
     * Finalmente, devolvemos true si se ha podido activar la estación o false si no se ha podido realizar la operación.
     */
    public synchronized boolean activarEstacion(String idEstacion){
        boolean aux=false;
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null){
            System.out.println("La estación con ID " + idEstacion + " no existe.");
        }
        else{
            if(!est.getEstado().equalsIgnoreCase("OUT_OF_SERVICE")){
                System.out.println("La estación no está en OUT_OF_SERVICE, su estado es " + est.getEstado() + ")");
            }
            else{
                est.setEstado(this.mapeoestaciones.containsKey(idEstacion) ? "AVAILABLE" : "DISCONNECTED");
                est.setCaudalactual(0.0);
                est.setOperarioId(null);
                actualizarEstacionEnSistema(est);
                aux=true;
                String mensajeKafkaEngine = "ACTIVAR/" + idEstacion;
                enviarmensajekafka(mensajeKafkaEngine, "central_engine");
            }
        }
        return aux;
    }

    /**
     * Gestiona un aviso de avería (fuga=true) o de recuperación (fuga=false) enviado por el Monitor.
     */
    public synchronized void notificarFuga(String idEstacion, boolean fuga){
        Estacion est = buscarEstacionMemoria(idEstacion);
        if(est == null) return;

        String operario = est.getOperarioId();

        if(fuga){
            //Un bloqueo manual tiene prioridad: no se pisa OUT_OF_SERVICE con LEAK
            if(!est.getEstado().equalsIgnoreCase("OUT_OF_SERVICE")){
                est.setEstado("LEAK");
            }
            est.setCaudalactual(0.0);
            est.setOperarioId(null);
            actualizarEstacionEnSistema(est);
            enviarAFO(operario, "LEAK_ALERT/" + idEstacion);
        }
        else{
            if(est.getEstado().equalsIgnoreCase("LEAK")){
                est.setEstado("AVAILABLE");
                actualizarEstacionEnSistema(est);
            }
            //Ya no hay operario asignado, así que no hay clave: lo reciben todos los FO
            enviarmensajekafka("LEAK_RESOLVED/" + idEstacion, "central_fo", null);
        }
    }

    public synchronized void procesarDesconexionEstacion(String idEstacion) {
        if(idEstacion!=null){
            Estacion est = buscarEstacionMemoria(idEstacion);
            if (est != null) {
                String estadoAnt = est.getEstado();
                String idOperarioAnt = est.getOperarioId();

                //El bloqueo manual se conserva aunque la estación se desconecte
                if(!estadoAnt.equalsIgnoreCase("OUT_OF_SERVICE")){
                    est.setEstado("DISCONNECTED");
                }
                est.setCaudalactual(0.0);
                est.setOperarioId(null);

                actualizarEstacionEnSistema(est);
                // Hay que avisar al operario de que se desconecta la estacoion que estaba en ese momento regando
                if ("WATERING".equalsIgnoreCase(estadoAnt) && idOperarioAnt != null) {
                    System.out.println("La estacion con el " + idEstacion + " se ha desconectado durante un riego activo.");

                    // Enviar aviso de fin/interrupción de riego al operario asignado vía Kafka
                    if (!idOperarioAnt.equalsIgnoreCase("CENTRAL")) {
                        String mensajeKafkaFO = "WATERING_END/" + idEstacion + "/0/0";
                        enviarmensajekafka(mensajeKafkaFO, "central_fo", idOperarioAnt);
                    }
                }
            }
        }
    }

    /**
     buscarEstacionMemoria permite consultar los datos en tiempo real desde la lista this.estaciones(yo puse getesatciones)
     sin perder rendimiento abriendo SQLite ni leer datos antiguos que pueden estar desfasados, mientras que actualizarEstacionEnSistema
     decide si debe hacer un INSERT o un UPDATE en SQLite, garantizan mediante synchronized que la lista en memoria
     y la base de datos se mantengan 100% idénticas y que esten protegidas frente a fallos cuando varios hilos ejecuten cambios a la vez.
     Al principio yo ponia guardar directamente desde la base de datos
     y no hacia el set o a veces solo hacia el set pero se me olvidaba a guardar en la base de datos ,y bueno como habia hecho dos diferentes
     tipos de crud uno para insertar nuevas estaciones y luego otro para actualizar estaciones pues solo actualizaba y no insertaba ,y era poco eficiente
     y es por ello por lo que después de revisar y darme cuenta de ciertos errores pues decidi hacer dos funciones a parte y estan separas porque no siempre
     se busca y se actualiza.
     */
    public Estacion buscarEstacionMemoria(String id) {
        if (id!=null) {
            for (Estacion e : this.estaciones) {
                if (e.getId().equalsIgnoreCase(id)) {
                    return e;
                }
            }
        }
        return null;
    }

    public synchronized boolean actualizarEstacionEnSistema(Estacion est) {
        boolean aux=false;
        if (est!=null) {
            Estacion existente=buscarEstacionMemoria(est.getId());
            if (existente==null) {
                this.estaciones.add(est);
                DatabaseManager.insertarEstacion(est);
            } else {
                DatabaseManager.actualizarEstacion(est);
            }
            aux=true;
        }
        return aux;
    }

}