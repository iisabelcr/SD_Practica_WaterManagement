package UA_SD;

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
                if(parts[0].equalsIgnoreCase("REGISTRO") && parts.length>=3){
                    Estacion estacionmdb=DatabaseManager.obtenerEstacion(parts[1]);
                    //Se comprueba si estaba antes en la base de datos
                    if(estacionmdb==null){
                        Estacion nueva =new Estacion(parts[1],parts[2]);
                        this.central.anadirestacion(nueva);
                        respuesta=" OK/ESTACION_REGISTRADA";
                        this.idEstacion=parts[1];
                        this.central.agregarmap(parts[1],this);
                    }
                    else{
                        this.central.actualEstadoEstacion(parts[1],parts[2]);
                        respuesta=" OK/ESTADO_ACTUALIZADO";
                    }
                }
                else if(parts[0].equalsIgnoreCase("ACTUALIZAR_ESTADO") && parts.length>=3){
                    boolean aux=this.central.actualEstadoEstacion(parts[1],parts[2]);
                    if(aux){
                        respuesta=" OK/ESTADO_ACTUALIZADO";
                    }
                    else{
                        respuesta = " STATUS/ERROR/Estacion no encontrada";
                    }
                }
                else if (parts[0].equalsIgnoreCase("RESPUESTA_RIEGO") && parts.length>=3) {
                    this.idEstacion = parts[1];
                    String estado = parts[2]; // Viene "OK" o "NOK"
                    if (estado.equalsIgnoreCase("OK")) {
                        System.out.println("Riego iniciado con éxito en la estación " + this.idEstacion);
                        this.central.actualEstadoEstacion(this.idEstacion, "REGANDO");
                        String mensajeKafka = " RESPUESTA_RIEGO/"  + this.idEstacion + "/OK";
                        this.central.enviarmensajekafka("central_fo",mensajeKafka);
                        respuesta=null;
                    }
                    else{
                        String mensajeKafka = " RESPUESTA_RIEGO/"  + this.idEstacion + "/NOK";
                        this.central.enviarmensajekafka("central_fo",mensajeKafka);
                        respuesta=null;
                    }
                }
                else if(parts[0].equalsIgnoreCase("DATOS_RIEGO") && parts.length>=4){
                    this.idEstacion=parts[1];
                    double caudal=Double.parseDouble(parts[2].replace(',', '.'));
                    double volumen=Double.parseDouble(parts[3].replace(',', '.'));
                    System.out.println(this.idEstacion+ caudal +volumen);
                    String mensajeKafka = " DATOS_RIEGO/" + this.idEstacion + "/" + caudal + "/" + volumen;
                    this.central.enviarmensajekafka("central_fo",mensajeKafka);
                    respuesta=null;
                }
                else if(parts[0].equalsIgnoreCase("FIN_RIEGO") && parts.length>=4){
                    this.idEstacion=parts[1];
                    this.central.actualEstadoEstacion(this.idEstacion, "AVAILABLE");
                    double tiempo=Double.parseDouble(parts[2].replace(',', '.'));
                    double volumen=Double.parseDouble(parts[3].replace(',', '.'));
                    System.out.println(this.idEstacion+ tiempo +volumen);
                    String mensajeKafka = " FIN_RIEGO/" + this.idEstacion + "/" + tiempo + "/" + volumen;
                    this.central.enviarmensajekafka("central_fo",mensajeKafka);
                    respuesta=null;
                }
                else if(parts[0].equalsIgnoreCase("FUGA") && parts.length>=2){
                    this.idEstacion = parts[1];
                    String estado = parts[2];
                    System.out.println("No hay riego porque hay una avería en la estación " + this.idEstacion);
                    this.central.actualEstadoEstacion(this.idEstacion,"LEAK");
                    String mensajeKafka = " ALERTA_FUGA/" + this.idEstacion;
                    this.central.enviarmensajekafka("central_fo",mensajeKafka);
                    respuesta=null;
                }
                else if(parts[0].equalsIgnoreCase("FUGA_RESUELTA") && parts.length>=2){
                    this.idEstacion = parts[1];
                    String estado = parts[2];
                    System.out.println("Ya no hay una avería en la estación " + this.idEstacion);
                    this.central.actualEstadoEstacion(this.idEstacion,"AVAILABLE");
                    String mensajeKafka = " FUGA_RESUELTA/" + this.idEstacion;
                    this.central.enviarmensajekafka("central_fo",mensajeKafka);
                    respuesta=null;
                }
                else{
                    respuesta = " STATUS/ERROR/Operacion no valida";
                }
            }
            else {
                respuesta = " STATUS/ERROR/Operacion no valida";
            }
        }
        else{
            respuesta = " STATUS/ERROR/Operacion no valida";
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
            if( this. socketCliente!=null && !this. socketCliente.isClosed()){
                try {
                    this.socketCliente.close();
                    System.out.println("Socket cerrado correctamente");
                    this.central.eliminarmap(this.idEstacion);
                    this.central.actualEstadoEstacion(this.idEstacion, "DISCONNECTED");
                } catch (IOException e) {
                    System.out.println("Socket no cerrado correctamente");
                }
            }
        }
    }
    public void enviarTrama(String s) {
        if(salida !=null && this.socketCliente != null && !this.socketCliente.isClosed()){
            System.out.println(s);
            System.out.flush();
        }
        else{
            System.err.println("o se pudo enviar la trama, socket no disponible");
        }
    }
}
