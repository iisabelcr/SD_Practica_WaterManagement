package org.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {
    //Busca en el disco si existe el archivo wm_central.db si no existe:SQLite lo crea automaticamente
    //en ese instante es como un archivo vacio en la carpeta de tu proyecto. A continuacion, cuando tu metodo
    //initDatabase() ejecuta los comandos, SQLite crea las tablas dentro de ese archivo wm-central.db recien
    //generado. Si ya existe:Simplemente abre la base de datos existente y deja las tablas como estaban.
    private static final String DB_URL = "jdbc:sqlite:wm_central.db";
    //Abre y retorna la conexión activa con el archivo wm_central.db.
    //(Si el archivo de la base de datos no existe, SQLite lo crea automáticamente en ese instante)
    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(DB_URL);
    }
    //Es el metodo encargado de ejecutar las sentencias SQL iniciales
    //para crear las tablas si la base de datos es nueva o no han sido creadas aún.
    public static void initDatabase() {
        String sqlEstaciones = """
            CREATE TABLE IF NOT EXISTS estaciones(
                id TEXT PRIMARY KEY,
                ubicacion TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'DESCONECTADA',
                caudalact REAL DEFAULT 0.0,
                volumenacum REAL DEFAULT 0.0,
            );
            """;

        String sqlOperarios = """
            CREATE TABLE IF NOT EXISTS operarios (
                id TEXT PRIMARY KEY,
                nombre TEXT NOT NULL
            );
            """;

        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sqlEstaciones);
            stmt.execute(sqlOperarios);
            System.out.println("[BD] Base de datos SQLite inicializada correctamente.");
        } catch (SQLException e) {
            System.err.println("[BD Error] Error inicializando BD: " + e.getMessage());
        }
    }
    public static void crearEstacion(Estacion myestacion){
        //Esto es para insertar dentro de los atributos de una estacion sus valores
        String sql="INSERT INTO estaciones(id,ubicacion,estado,caudalact,volumenacum) VALUES (?,?,?,?,?)";
        //Intentamos ver si funciona la conexion
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)){
            //Van del 1 al 6 porque son los ? que van en orden
            pstmt.setString(1,myestacion.getId());
            pstmt.setString(2,myestacion.getUbicacion());
            pstmt.setString(3,myestacion.getEstado());
            pstmt.setDouble(4,myestacion.getCaudalactual());
            pstmt.setDouble(5,myestacion.getVolumenacum());
            //Esto es para ejecutar el update
            pstmt.executeUpdate();
        }catch (SQLException e) {
            System.err.println("[BD Error] Error al crear la base de datos BD: " + e.getMessage());
        }
    }
    public static void leerEstacion(Estacion myestacion){
        String sql="SELECT id,ubicacion,estado,caudalact,volumenacum FROM estaciones";
        try( //1. Abre el canal de comunicación con el archivo SQLite.
             Connection conn = connect();
             //2. El objeto que transporta y ejecuta la orden SQL en la base de datos. Podria haber puesto PreparedStatement
             //Pero como esta vez no estaba utilizando en el string sql lo de ? no hace falta
             Statement stmt  = conn.createStatement();
             // 3. El contenedor que recibe y guarda todas las filas devueltas por SQLite.Funcionaria como un cursor
             ResultSet rs = stmt.executeQuery(sql) ){
            while (rs.next()){
                String id=rs.getString("id");
                String ubicacion=rs.getString("ubicacion");
                String estado=rs.getString("estado");
                Double caudal=rs.getDouble("caudalact");
                Double volumen=rs.getDouble("volumenacum");
                //Recordatorio println no va con comas entonces utilizo printfn
                //Es estupido que haya puesto en variables pero en fin
                System.out.printf(id,ubicacion,estado,caudal,volumen);
            }
        }catch (SQLException e){
            System.err.println("[BD Error] Error al leer la base de datos BD: " + e.getMessage());
        }
    }
}
