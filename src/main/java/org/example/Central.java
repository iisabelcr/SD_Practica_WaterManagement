package org.example;

import java.util.ArrayList;
import java.util.List;

public class Central {
    List<Estacion> estaciones=new ArrayList<>();
    List<Operario> operarios=new ArrayList<>();

    public Central(List<Estacion> estaciones,List<Operario> operarios){
        this.estaciones=estaciones;
        this.operarios=operarios;
    }
    public List<Operario> getOperarios() {
        return operarios;
    }

    public void setOperarios(List<Operario> operarios) {
        if(operarios!=null){
            if (this.operarios == null) {
                this.operarios = new ArrayList<>();
            }
            for(int i=0;i<operarios.size();i++){
                this.operarios.add(operarios.get(i));
            }
        }
    }

    public List<Estacion> getaestaciones() {
        return estaciones;
    }

    public void setestaciones(List<Estacion> estaciones) {
        if(estaciones!=null){
            if (this.estaciones == null) {
                this.estaciones = new ArrayList<>();
            }
            for(int i=0;i<estaciones.size();i++){
                this.estaciones.add(estaciones.get(i));
            }
        }
    }
}
