package org.example;

public class Estacion {
    String id;
    String ubicacion;
    String estado;
    double caudalactual;
    double volumenacum;
    //Me faltaria pillar el id del operario
    //Y no se si lo de detector como tal es un atributo o no y tambien lo de actuador
    public Estacion(String id,String ubicacion,String estado,double caudalactual,double volumenacum){
        this.id=id;
        this.ubicacion=ubicacion;
        this.estado=estado;
        this.caudalactual=caudalactual;
        this.volumenacum=volumenacum;
    }
    public double getVolumenacum() {
        return volumenacum;
    }

    public void setVolumenacum(double volumenacum) {
        this.volumenacum = volumenacum;
    }

    public double getCaudalactual() {
        return caudalactual;
    }

    public void setCaudalactual(double caudalactual) {
        this.caudalactual = caudalactual;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getUbicacion() {
        return ubicacion;
    }

    public void setUbicacion(String ubicacion) {
        this.ubicacion = ubicacion;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
