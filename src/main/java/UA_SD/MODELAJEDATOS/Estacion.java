package UA_SD.MODELAJEDATOS;

public class Estacion {
    private String id;
    private String ubicacion;
    //AVAILABLE,WATERING,LEAK,OUT_OF_SERVICE,OFFLINE estos son los estados
    private String estado;
    private double caudalactual;
    private double volumenacum;
    //La estación debe identificar al operario que realiza el riego y enviar su identificación a la Central para que esta pueda mostrarla y registrar la actividad.
    private String operarioId;
    //El constructor completo es para cuando leo los datos desde la base de datos SQLite
    public Estacion(String id,String ubicacion,String estado,double caudalactual,double volumenacum,String operarioId){
        this.id=id;
        this.ubicacion=ubicacion;
        this.estado=estado;
        this.caudalactual=caudalactual;
        this.volumenacum=volumenacum;
        this.operarioId=operarioId;
    }
    //Cuando se crea una estacion por primera vez(es decir la de alta)se utiliza el constructor simplificado
    public Estacion(String id,String ubicacion){
        this.id=id;
        this.ubicacion=ubicacion;
        this.estado="AVAILABLE";
        this.caudalactual=0.0;
        this.volumenacum=0.0;
        this.operarioId=null;
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
    public String getOperarioId() {
        return operarioId;
    }

    public void setOperarioId(String operarioId) {
        this.operarioId = operarioId;
    }
}
