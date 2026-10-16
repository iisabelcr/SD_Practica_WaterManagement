package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.net.Socket;
import java.net.ServerSocket;

public class WM_Central {
    private List<Estacion> estaciones;
    private List<Operario> operarios;
    public WM_Central(){
        this.estaciones=new ArrayList<>();
        this.operarios=new ArrayList<>();
    }
    public WM_Central(List<Estacion> estaciones,List<Operario> operarios){
        this.estaciones=estaciones;
        this.operarios=operarios;
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
    public void actualEstadoEstacion(String id,String nueEstado){
        boolean aux=false;
        for(int i=0;i<estaciones.size() || !aux ;i++){
            if(estaciones.get(i).getId().equalsIgnoreCase(id)){
                Estacion est = estaciones.get(i);
                est.setEstado(nueEstado);
                DatabaseManager.actualizarEstacion(est);
                aux=true;
            }
        }
    }
    public void setestaciones(List<Estacion> estaciones) {
        if (estaciones != null) {
            this.estaciones = estaciones;
        }
    }
    public static void main(String[] args){
        //Se llama al initDatabase para crear la tabla de los operarios y de lasa estaciones
        DatabaseManager.initDatabase();
        //Luego se crea una instancia de la central
        //Se leen todas las estaciones que hay en el databasemanager y se pone en el set
        WM_Central central= new WM_Central();
        central.setestaciones(DatabaseManager.leerTodasEstaciones());
        //Se crea el socket
        try(ServerSocket servidorsocket= new ServerSocket(8080)){
            System.out.println("Se ha establecido un socket");
            while(true){
                servidorsocket.accept();
                //ClientHandler cliente=new ClientHandler();

            }
        }catch(IOException e){
            System.err.println("No se ha  podido establer un socket");
        }

    }
}
