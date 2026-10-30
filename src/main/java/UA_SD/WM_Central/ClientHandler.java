package UA_SD.WM_Central;

import UA_SD.MODELAJEDATOS.Estacion;

import java.io.*;
import java.net.Socket;

public class ClientHandler implements Runnable{
    private Socket socketCliente;
    private WM_Central central;
    private BufferedReader entrada;
    private PrintWriter salida;
    private String idEstacion;
    /*
    Los canales de texto sirven como "tuberías" para que tu programa mande y reciba palabras a través
    del cable de internet. Tienes que prepararlos dentro del constructor (en el momento de nacer el objeto)
    porque es como conectar los dos extremos de la tubería antes de abrir la llave del agua; si el hilo empieza
    a correr y las tuberías no están bien conectadas, el programa intentará hablarle a la nada, se romperá por
    completo en segundo plano y no sabrás ni por qué falló.
    * */
    public ClientHandler(Socket s, WM_Central w){
        socketCliente=s;
        central=w;
        try{
            InputStream aux = s.getInputStream();
            InputStreamReader aux1= new InputStreamReader(aux);
            this.entrada=new BufferedReader(aux1);
            OutputStream aux2=  s.getOutputStream();
            this.salida= new PrintWriter(aux2,true);
        }catch(Exception e){
            System.out.println("Error de lectura y/o de escritura  en el socket: " + e);
        }
    }
    public String leerSocket(){
        try{
            return entrada.readLine();
        }catch(IOException e){
            System.out.println("Error en leer lo de la entrada");
            //Se devuelve null (y no "") para que el while del run() termine cuando se cae la conexión
            return null;
        }
    }
    public void escribirSocket(String mensaje){
        //PrintWriter nunca lanza excepciones, por eso ya no hace falta el try/catch
        salida.println(mensaje);
    }
    public String processtrama(String trama){
        String respuesta;
        if(trama!=null){
            String[] parts =trama.split("/");
            if (parts.length>=1){
                if(parts[0].equalsIgnoreCase("REGISTER") && parts.length>=3){
                    String id = parts[1];
                    String ubicacion = parts[2];

                    Estacion estacionMemoria = this.central.buscarEstacionMemoria(id);
                    if (estacionMemoria == null) {
                        Estacion nueva = new Estacion(id, ubicacion);
                        this.central.actualizarEstacionEnSistema(nueva);
                        respuesta = "OK/STATION_REGISTERED";
                    } else {
                        //Ya estaba en memoria (cargada de la BD o es una reconexión del Monitor).
                        //parts[2] es la ubicación, no un estado, así que ya no se usa en setEstado.
                        //Solo pasa a AVAILABLE si estaba DISCONNECTED: no se pisa OUT_OF_SERVICE ni LEAK
                        if (estacionMemoria.getEstado().equalsIgnoreCase("DISCONNECTED")) {
                            estacionMemoria.setEstado("AVAILABLE");
                        }
                        this.central.actualizarEstacionEnSistema(estacionMemoria);
                        respuesta = "OK/STATUS_UPDATED";
                    }

                    //Esto se hace siempre (nueva o ya conocida): si no, el handler no queda en el mapa
                    this.idEstacion = id;
                    this.central.agregarmap(id, this);
                }
                else if (parts[0].equalsIgnoreCase("UPDATE_STATUS") && parts.length >= 3) {
                    Estacion est = this.central.buscarEstacionMemoria(parts[1]);
                    if (est != null) {
                        est.setEstado(parts[2]);
                        this.central.actualizarEstacionEnSistema(est);
                        respuesta = "OK/STATUS_UPDATED";
                    } else {
                        respuesta = "STATUS/ERROR/Station not found";
                    }
                }
                else if (parts[0].equalsIgnoreCase("LEAK")) {
                    if (idEstacion == null) {
                        respuesta = "STATUS/ERROR/Estacion no registrada";
                    } else {
                        System.out.println("No hay riego porque hay una avería en la estación " + idEstacion);
                        //Cambia el estado a LEAK y avisa al operario que estaba regando
                        this.central.notificarFuga(idEstacion, true);
                        respuesta = "OK/LEAK_REGISTERED";
                    }
                } else if (parts[0].equalsIgnoreCase("LEAK_RESOLVED")) {
                    if (idEstacion == null) {
                        respuesta = "STATUS/ERROR/Estacion no registrada";
                    } else {
                        System.out.println("Ya no hay una avería en la estación " + idEstacion);
                        this.central.notificarFuga(idEstacion, false);
                        respuesta = "OK/LEAK_RESOLVED_REGISTERED";
                    }
                }
                else{
                    respuesta = "STATUS/ERROR/Operacion no valida";
                }
            }
            else {
                respuesta = "STATUS/ERROR/Operacion no valida";
            }
        }
        else{
            respuesta = "STATUS/ERROR/Operacion no valida";
        }
        return respuesta;
    }
    @Override
    public void run() {
        String resultado;
        String Cadena;
        try{
            while((Cadena=leerSocket())!=null){
                System.out.println("Trama recibida" + Cadena);
                resultado=this.processtrama(Cadena);
                System.out.println("El resultado de la trama recibida"+resultado);
                this.escribirSocket(resultado);
            }
        }catch(Exception e){
            System.err.println("Error al realizar el run");
        }
        finally {
            if (this.socketCliente != null && !this.socketCliente.isClosed()) {
                try {
                    this.socketCliente.close();
                    System.out.println("Socket cerrado correctamente.");
                } catch (IOException e) {
                    System.err.println("Error al cerrar el socket: " + e.getMessage());
                }
            }
            //Esto es por si la estacion llego realmente a conectarese
            if (this.idEstacion != null && !this.idEstacion.trim().isEmpty()) {
                System.out.println("Procesando desconexión de la estación: " + this.idEstacion);
                //Solo se procesa la desconexión si este handler sigue siendo el activo en el mapa.
                //Si el Monitor ya reconectó con otro handler, este no debe borrar al nuevo ni marcarla DISCONNECTED
                if (this.central.eliminarmap(this.idEstacion, this)) {
                    this.central.procesarDesconexionEstacion(this.idEstacion);
                }
            } else {
                System.out.println("Cliente desconectado sin haberse registrado previamente.");
            }
        }
    }
    //Para enviar una trama necesito el productor el mensaje que ya esta aqui y el topico
    //(Se publica en Kafka; el socket solo se comprueba para saber si la estación sigue conectada)
    public void enviarTrama(String s,String topic) {
        if(salida !=null && this.socketCliente != null && !this.socketCliente.isClosed()){
            central.enviarmensajekafka(s,topic);
        }
        else{
            System.err.println("No se pudo enviar la trama, socket no disponible");
        }
    }
}

