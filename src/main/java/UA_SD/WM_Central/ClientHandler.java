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
        String aux = "";
        try{
           aux= entrada.readLine();
        }catch(IOException e){
            System.out.println("Error en leer lo de la entrada");
        }
        return aux;
    }
    public void escribirSocket(String mensaje){
        try{
            salida.println(mensaje);
        }catch(Exception e){
            System.err.println("Error en leer lo de la entrada");
        }
    }
    public String processtrama(String trama){
        String respuesta;
        if(trama!=null){
            String[] parts =trama.split("/");
            if (parts.length>=1){
                if(parts[0].equalsIgnoreCase("REGISTER") && parts.length>=3){
                    Estacion estacionMemoria = this.central.buscarEstacionMemoria(parts[1]);
                    if (estacionMemoria == null) {
                        Estacion nueva = new Estacion(parts[1], parts[2]);
                        this.central.actualizarEstacionEnSistema(nueva);
                        respuesta = "OK/STATION_REGISTERED";
                        this.idEstacion = parts[1];
                        this.central.agregarmap(parts[1], this);
                    } else {
                        estacionMemoria.setEstado(parts[2]);
                        this.central.actualizarEstacionEnSistema(estacionMemoria);
                        respuesta = "OK/STATUS_UPDATED";
                    }
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
                    System.out.println("No hay riego porque hay una avería en la estación " + idEstacion);
                    String mensajeKafka = "LEAK_ALERT/" + idEstacion;
                    enviarTrama(mensajeKafka,"central_fo");
                    respuesta = "OK/LEAK_REGISTERED";
                } else if (parts[0].equalsIgnoreCase("LEAK_RESOLVED")) {
                    System.out.println("Ya no hay una avería en la estación " + idEstacion);
                    String mensajeKafka = "LEAK_RESOLVED/" + idEstacion;
                    enviarTrama(mensajeKafka,"central_fo");
                    respuesta = "OK/LEAK_RESOLVED_REGISTERED";
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
                this.central.eliminarmap(this.idEstacion);
                this.central.procesarDesconexionEstacion(this.idEstacion);
            } else {
                System.out.println("Cliente desconectado sin haberse registrado previamente.");
            }
        }
    }
    //Para enviar una trama necesito el productor el mensaje que ya esta aqui y el topico
    public void enviarTrama(String s,String topic) {
        if(salida !=null && this.socketCliente != null && !this.socketCliente.isClosed()){
          central.enviarmensajekafka(s,topic);
        }
        else{
            System.err.println("No se pudo enviar la trama, socket no disponible");
        }
    }
}
