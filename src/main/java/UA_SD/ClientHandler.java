package UA_SD;

import java.io.*;
import java.net.Socket;

public class ClientHandler implements Runnable{
    private Socket socketCliente;
    private WM_Central central;
    private BufferedReader entrada;
    private PrintWriter salida;
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
            String[] parts =trama.split("#");
            if (parts.length>=3){
                if(parts[0].compareToIgnoreCase("REGISTRO_ESTACION")==0){
                    Estacion nueva =new Estacion(parts[1],parts[2]);
                    this.central.anadirestacion(nueva);
                    respuesta="OK#ESTACION_REGISTRADA";
                }
                else if(parts[0].compareToIgnoreCase("ACTUALIZAR_ESTADO")==0){
                    this.central.actualEstadoEstacion(parts[1],parts[2]);
                    respuesta="OK#ESTADO_ACTUALIZADO";
                }
                else{
                    respuesta = "STATUS#ERROR#Operacion no valida";
                }
            }
            else {
                respuesta = "STATUS#ERROR#Operacion no valida";
            }
        }
        else{
            respuesta = "STATUS#ERROR#Operacion no valida";
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
                } catch (IOException e) {
                    System.out.println("Socket no cerrado correctamente");
                }
            }
        }
    }
}
