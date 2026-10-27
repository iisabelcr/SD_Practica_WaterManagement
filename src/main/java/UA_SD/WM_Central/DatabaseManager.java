package UA_SD.WM_Central;
import UA_SD.MODELAJEDATOS.Estacion;
import UA_SD.MODELAJEDATOS.Operario;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {
    //Busca en el disco si existe el archivo wm_central.db si no existe:SQLite lo crea automaticamente
    //en ese instante es como un archivo vacio en la carpeta de tu proyecto. A continuacion, cuando tu metodo
    //initDatabase() ejecuta los comandos, SQLite crea las tablas dentro de ese archivo wm-central.db recien
    //generado. Si ya existe:Simplemente abre la base de datos existente y deja las tablas como estaban.
    private static final String DB_URL = "jdbc:sqlite:" + System.getenv().getOrDefault("DB_PATH", "wm_central.db");
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
                estado TEXT NOT NULL DEFAULT 'AVAILABLE',
                caudalact REAL DEFAULT 0.0,
                volumenacum REAL DEFAULT 0.0,
                operarioId TEXT 
            );
            """;
//No se si el operarioID tiene que ser not null o si lo puede ser
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
    public static void insertarOperarios(Operario myoperario){
        String sql="INSERT INTO operarios (id,nombre) VALUES (?,?)";
        try(Connection conn = connect();
            PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1,myoperario.getId());
            pstmt.setString(2,myoperario.getNombre());
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            System.err.println("[BD Error] Error al insertar en la base de datos BD: " + e.getMessage());
        }
    }
    public static void insertarEstacion(Estacion myestacion){
        //Esto es para insertar dentro de los atributos de una estacion sus valores
        String sql="INSERT INTO estaciones (id,ubicacion,estado,caudalact,volumenacum,operarioId) VALUES (?,?,?,?,?,?)";
        //Intentamos ver si funciona la conexion
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)){
            //Van del 1 al 6 porque son los ? que van en orden
            pstmt.setString(1,myestacion.getId());
            pstmt.setString(2,myestacion.getUbicacion());
            pstmt.setString(3,myestacion.getEstado());
            pstmt.setDouble(4,myestacion.getCaudalactual());
            pstmt.setDouble(5,myestacion.getVolumenacum());
            pstmt.setString(6, myestacion.getOperarioId());
            //Esto es para ejecutar el update
            pstmt.executeUpdate();
        }catch (SQLException e) {
            System.err.println("[BD Error] Error al insertar en la base de datos BD: " + e.getMessage());
        }
    }
    //Esta no se pa que muy bien la quiero
    public static List<Operario> leerTodasOperario(){
        List<Operario> lista = new ArrayList<>();
        String sql="SELECT id,nombre FROM operarios";
        try( //1. Abre el canal de comunicación con el archivo SQLite.
             Connection conn = connect();
             //2. El objeto que transporta y ejecuta la orden SQL en la base de datos. Podria haber puesto PreparedStatement
             //Pero como esta vez no estaba utilizando en el string sql lo de ? no hace falta
             Statement stmt  = conn.createStatement();
             // 3. El contenedor que recibe y guarda todas las filas devueltas por SQLite. Funcionaria como un cursor
             ResultSet rs = stmt.executeQuery(sql) ){
            while (rs.next()){
                String id=rs.getString("id");
                String nombre=rs.getString("nombre");
                System.out.printf("Operario -> ID: %s | Nombre: %s%n", id, nombre);
                lista.add(new Operario(id,nombre));
            }
        }catch (SQLException e){
            System.err.println("[BD Error] Error al leer la base de datos BD: " + e.getMessage());
        }
        return lista;
    }
    public static List<Estacion> leerTodasEstaciones(){
        List<Estacion> lista = new ArrayList<>();
        String sql="SELECT id,ubicacion,estado,caudalact,volumenacum FROM estaciones";
        try( //1. Abre el canal de comunicación con el archivo SQLite.
             Connection conn = connect();
             //2. El objeto que transporta y ejecuta la orden SQL en la base de datos. Podria haber puesto PreparedStatement
             //Pero como esta vez no estaba utilizando en el string sql lo de ? no hace falta
             Statement stmt  = conn.createStatement();
             // 3. El contenedor que recibe y guarda todas las filas devueltas por SQLite. Funcionaria como un cursor
             ResultSet rs = stmt.executeQuery(sql) ){
            while (rs.next()){
                String id=rs.getString("id");
                String ubicacion=rs.getString("ubicacion");
                String estado=rs.getString("estado");
                double caudal=rs.getDouble("caudalact");
                double volumen=rs.getDouble("volumenacum");
                String operarioId = rs.getString("operarioId");
                //Recordatorio println no va con comas entonces utilizo printfn
                //Es estupido que haya puesto en variables pero en fin
                System.out.printf("Estación -> ID: %s | Ubicación: %s | Estado: %s | Caudal: %.2f | Vol: %.2f%n",
                        id, ubicacion, estado, caudal, volumen);
                lista.add(new Estacion(id, ubicacion, estado, caudal, volumen, operarioId));
            }
        }catch (SQLException e){
            System.err.println("[BD Error] Error al leer la base de datos BD: " + e.getMessage());
        }
        return lista;
    }
    public static void borrarOperario(Operario myoperario){
        String sql="DELETE FROM operario where id=?";
        try(
            Connection conn = connect();
            PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1,myoperario.getId());
            pstmt.executeUpdate();
        }catch (SQLException e){
            System.err.println("[BD Error] Error al borrar: " + e.getMessage());
        }
    }
    public static void borrarEstacion(Estacion myestacion){
        String sql="DELETE FROM estaciones where id=?";
        try(
            Connection conn = connect();
            PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1, myestacion.getId());
            pstmt.executeUpdate();
        }catch (SQLException e){
            System.err.println("[BD Error] Error al borrar: " + e.getMessage());
        }
    }
    public static void actualizarOperario(Operario myoperario){
        String sql ="UPDATE operarios set nombre = ? where id = ?";
        try(Connection conn = connect();
        PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1,myoperario.getNombre());
            pstmt.setString(2, myoperario.getId());
            pstmt.executeUpdate();
        }catch (SQLException e){
            System.err.println("[BD Error] Error al actualizar la base de datos: " + e.getMessage());
        }
    }
    public static void actualizarEstacion(Estacion myestacion){
        String sql="UPDATE estaciones set ubicacion = ?, estado = ?,caudalact = ?,volumenacum = ?,operarioId = ? WHERE id = ?";
        try(Connection conn= connect();
            PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1,myestacion.getUbicacion());
            pstmt.setString(2,myestacion.getEstado());
            pstmt.setDouble(3,myestacion.getCaudalactual());
            pstmt.setDouble(4,myestacion.getVolumenacum());
            pstmt.setString(5, myestacion.getOperarioId());
            pstmt.setString(6, myestacion.getId());
            //Esto es para ejecutar el update
            pstmt.executeUpdate();
        }
        catch (SQLException e){
            System.err.println("[BD Error] Error al actualizar la base de datos: " + e.getMessage());
        }
    }
    public static Operario obtenerOperario(String id){
        String sql="SELECT id,nombre FROM operarios where id = ?";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Operario(
                            rs.getString("id"),
                            rs.getString("nombre")
                    );
                }
            }
        } catch (SQLException e) {
            System.err.println("[BD Error] Error al obtener el operario: " + e.getMessage());
        }
        return null;
    }
    public static Estacion obtenerEstacion(String id){
        String sql="SELECT id,ubicacion,estado,caudalact,volumenacum,operarioId FROM estaciones where id = ?";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1,id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Estacion(
                            rs.getString("id"),
                            rs.getString("ubicacion"),
                            rs.getString("estado"),
                            rs.getDouble("caudalact"),
                            rs.getDouble("volumenacum"),
                            rs.getString("operarioId")
                    );
                }
            }
        } catch (SQLException e) {
            System.err.println("[BD Error] Error al obtener la estación: " + e.getMessage());
        }
        return null;
    }

}
