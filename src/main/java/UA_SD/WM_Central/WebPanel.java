package UA_SD.WM_Central;

import UA_SD.MODELAJEDATOS.Estacion;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Panel web de monitorización de CENTRAL. Usa solo el servidor HTTP incluido en el JDK.
 *
 * Endpoints:
 *   GET  /                 -> la página
 *   GET  /api/estado       -> estaciones + eventos recientes (JSON)
 *   POST /api/riego        -> ?id=WS-1&segundos=30
 *   POST /api/bloquear     -> ?id=WS-1   (id=ALL para todas)
 *   POST /api/activar      -> ?id=WS-1   (id=ALL para todas)
 *
 * Si existe la variable de entorno PANEL_TOKEN, las acciones POST exigen ?token=...
 */
public class WebPanel {
    private final WM_Central central;
    private final HttpServer server;
    private final String token = System.getenv("PANEL_TOKEN");

    //Registro de eventos: se generan comparando el estado de cada estación entre una vuelta y la siguiente,
    //así no hace falta tocar WM_Central para saber qué ha pasado
    private final ConcurrentLinkedDeque<String> eventos = new ConcurrentLinkedDeque<>();
    private final Map<String, String> ultimoEstado = new ConcurrentHashMap<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public WebPanel(WM_Central central, int puerto) throws IOException {
        this.central = central;
        this.server = HttpServer.create(new InetSocketAddress(puerto), 0);
        server.createContext("/", ex -> proteger(ex, this::servirPagina));
        server.createContext("/api/estado", ex -> proteger(ex, this::apiEstado));
        server.createContext("/api/riego", ex -> proteger(ex, this::apiRiego));
        server.createContext("/api/bloquear", ex -> proteger(ex, this::apiBloquear));
        server.createContext("/api/activar", ex -> proteger(ex, this::apiActivar));
        server.setExecutor(Executors.newFixedThreadPool(4));
    }

    public static int puertoPorDefecto() {
            return 8080;
        
    }

    public void iniciar() {
        registrar("Panel web iniciado");
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        ses.scheduleAtFixedRate(this::detectarCambios, 0, 500, TimeUnit.MILLISECONDS);
        server.start();
        System.out.println("[WEB] Panel disponible en el puerto " + server.getAddress().getPort());
    }

    // ---------------------------------------------------------------- eventos

    private void registrar(String texto) {
        eventos.addFirst(LocalTime.now().format(HORA) + "  " + texto);
        while (eventos.size() > 60) eventos.pollLast();
    }

    private void detectarCambios() {
        try {
            for (Estacion e : central.getaestaciones()) {
                String prev = ultimoEstado.put(e.getId(), e.getEstado());
                if (prev == null) {
                    registrar(e.getId() + " (" + e.getUbicacion() + ") en el sistema: " + e.getEstado());
                } else if (!prev.equals(e.getEstado())) {
                    registrar(e.getId() + ": " + prev + " -> " + e.getEstado());
                }
            }
        } catch (Exception ex) {
            System.err.println("[WEB] Error detectando cambios: " + ex.getMessage());
        }
    }

    // ---------------------------------------------------------------- handlers

    private interface Manejador {
        void atender(HttpExchange ex) throws Exception;
    }

    private void proteger(HttpExchange ex, Manejador m) throws IOException {
        try {
            m.atender(ex);
        } catch (Exception e) {
            System.err.println("[WEB] Error: " + e);
            enviar(ex, 500, "application/json", "{\"ok\":false,\"error\":\"Error interno\"}");
        } finally {
            ex.close();
        }
    }

    private void servirPagina(HttpExchange ex) throws IOException {
        if (!ex.getRequestURI().getPath().equals("/")) {
            enviar(ex, 404, "text/plain", "No encontrado");
            return;
        }
        enviar(ex, 200, "text/html; charset=utf-8", HTML);
    }

    private void apiEstado(HttpExchange ex) throws IOException {
        StringBuilder sb = new StringBuilder("{\"estaciones\":[");
        List<Estacion> lista = central.getaestaciones();
        boolean primera = true;
        for (Estacion e : lista) {
            if (!primera) sb.append(',');
            primera = false;
            sb.append("{\"id\":").append(js(e.getId()))
                    .append(",\"ubicacion\":").append(js(e.getUbicacion()))
                    .append(",\"estado\":").append(js(e.getEstado()))
                    .append(",\"caudal\":").append(num(e.getCaudalactual()))
                    .append(",\"volumen\":").append(num(e.getVolumenacum()))
                    .append(",\"operario\":").append(js(e.getOperarioId()))
                    .append(",\"conectada\":").append(central.getMapeoestaciones().containsKey(e.getId()))
                    .append('}');
        }
        sb.append("],\"eventos\":[");
        primera = true;
        for (String ev : eventos) {
            if (!primera) sb.append(',');
            primera = false;
            sb.append(js(ev));
        }
        sb.append("]}");
        enviar(ex, 200, "application/json", sb.toString());
    }

    private void apiRiego(HttpExchange ex) throws IOException {
        Map<String, String> q = accion(ex);
        if (q == null) return;
        String id = q.getOrDefault("id", "");
        String seg = q.getOrDefault("segundos", "");
        String error = central.iniciarRiegoManual(id, seg, "CENTRAL");
        if (error == null) {
            registrar("Riego manual solicitado desde el panel en " + id + " (" + seg + " s)");
            enviar(ex, 200, "application/json", "{\"ok\":true}");
        } else {
            enviar(ex, 200, "application/json", "{\"ok\":false,\"error\":" + js(error) + "}");
        }
    }

    private void apiBloquear(HttpExchange ex) throws IOException {
        cambiarEstado(ex, true);
    }

    private void apiActivar(HttpExchange ex) throws IOException {
        cambiarEstado(ex, false);
    }

    private void cambiarEstado(HttpExchange ex, boolean bloquear) throws IOException {
        Map<String, String> q = accion(ex);
        if (q == null) return;
        String id = q.getOrDefault("id", "");
        int hechas = 0;
        if (id.equalsIgnoreCase("ALL")) {
            for (Estacion e : central.getaestaciones()) {
                boolean ok = bloquear ? central.bloquearEstacion(e.getId()) : central.activarEstacion(e.getId());
                if (ok) hechas++;
            }
            registrar((bloquear ? "Bloqueo" : "Activación") + " de todas las estaciones desde el panel (" + hechas + ")");
            enviar(ex, 200, "application/json", "{\"ok\":true}");
        } else {
            boolean ok = bloquear ? central.bloquearEstacion(id) : central.activarEstacion(id);
            if (ok) {
                registrar((bloquear ? "Bloqueo" : "Activación") + " de " + id + " desde el panel");
                enviar(ex, 200, "application/json", "{\"ok\":true}");
            } else {
                String motivo = bloquear ? "No se pudo bloquear (no existe o ya está fuera de servicio)"
                        : "No se pudo activar (no existe o no está fuera de servicio)";
                enviar(ex, 200, "application/json", "{\"ok\":false,\"error\":" + js(motivo) + "}");
            }
        }
    }

    /** Valida método y token. Devuelve los parámetros o null si ya ha respondido con error. */
    private Map<String, String> accion(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equalsIgnoreCase("POST")) {
            enviar(ex, 405, "application/json", "{\"ok\":false,\"error\":\"Usa POST\"}");
            return null;
        }
        Map<String, String> q = parametros(ex.getRequestURI().getRawQuery());
        if (token != null && !token.isEmpty() && !token.equals(q.get("token"))) {
            enviar(ex, 403, "application/json", "{\"ok\":false,\"error\":\"Token incorrecto\"}");
            return null;
        }
        return q;
    }

    // ---------------------------------------------------------------- utilidades

    private static Map<String, String> parametros(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null) return m;
        for (String par : raw.split("&")) {
            int i = par.indexOf('=');
            if (i > 0) {
                m.put(URLDecoder.decode(par.substring(0, i), StandardCharsets.UTF_8),
                        URLDecoder.decode(par.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return m;
    }

    private static String num(double d) {
        return String.format(java.util.Locale.US, "%.2f", d);
    }

    private static String js(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    private static void enviar(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        byte[] datos = cuerpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, datos.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(datos);
        }
    }

    // ---------------------------------------------------------------- página

    private static final String HTML = """
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement - Central</title>
<style>
  :root { --bg:#0b1a22; --panel:#10242e; --line:#1d3a47; --txt:#d6e6ec; --mut:#6f8b97;
          --ok:#2fbf71; --leak:#e5484d; --oos:#f5a524; --off:#8a949a; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px/1.4 system-ui,Segoe UI,Roboto,sans-serif; }
  header { padding:16px 24px; border-bottom:1px solid var(--line); display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between; }
  header h1 { margin:0; font-size:18px; font-weight:600; }
  header h1 span { color:var(--mut); font-weight:400; }
  .chips { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { padding:4px 12px; border:1px solid var(--line); border-radius:999px; background:var(--panel); font-size:12px; }
  .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:6px; }
  main { display:grid; grid-template-columns:minmax(0,2fr) minmax(280px,1fr); gap:16px; padding:16px 24px; }
  @media (max-width:900px) { main { grid-template-columns:1fr; } }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  .card h2 { margin:0; padding:10px 14px; font-size:12px; letter-spacing:.08em; text-transform:uppercase; color:var(--mut); border-bottom:1px solid var(--line); }
  .scroll { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; }
  th, td { padding:9px 14px; text-align:left; white-space:nowrap; }
  th { color:var(--mut); font-weight:500; font-size:12px; border-bottom:1px solid var(--line); }
  tbody tr { cursor:pointer; border-bottom:1px solid #153039; }
  tbody tr:hover { background:#14303b; }
  tbody tr.sel { outline:2px solid #3b8fb0; outline-offset:-2px; }
  td.num { font-variant-numeric:tabular-nums; }
  .badge { display:inline-block; min-width:130px; text-align:center; padding:3px 10px; border-radius:6px; font-weight:600; font-size:12px; color:#06130c; }
  .AVAILABLE { background:var(--ok); }
  .WATERING { background:var(--ok); animation:blink 1s steps(2,start) infinite; }
  .LEAK { background:var(--leak); color:#fff; }
  .OUT_OF_SERVICE { background:var(--oos); }
  .DISCONNECTED { background:var(--off); }
  @keyframes blink { 50% { opacity:.35; } }
  .side { display:flex; flex-direction:column; gap:16px; }
  .body { padding:14px; }
  .row { display:flex; gap:8px; margin-top:10px; flex-wrap:wrap; }
  label { display:block; color:var(--mut); font-size:12px; margin-bottom:4px; }
  input { width:100%; padding:8px 10px; background:var(--bg); color:var(--txt); border:1px solid var(--line); border-radius:6px; font:inherit; }
  button { flex:1; padding:9px 12px; border-radius:6px; border:1px solid var(--line); background:#143543; color:var(--txt); font:inherit; cursor:pointer; }
  button:hover { filter:brightness(1.25); }
  button.go { border-color:var(--ok); color:var(--ok); }
  button.stop { border-color:var(--oos); color:var(--oos); }
  button.all { font-size:12px; color:var(--mut); }
  #sel { font-weight:600; font-size:16px; }
  #selinfo { color:var(--mut); margin-top:2px; }
  #msg { margin-top:10px; min-height:18px; font-size:13px; }
  #msg.err { color:#ff8a8d; } #msg.ok { color:var(--ok); }
  #log { max-height:300px; overflow:auto; padding:10px 14px; font:12px/1.6 ui-monospace,Consolas,monospace; }
  #log div { border-bottom:1px solid #153039; padding:2px 0; }
  #conn { font-size:12px; color:var(--mut); }
  #conn.bad { color:#ff8a8d; }
</style>
</head>
<body>
<header>
  <h1>WaterManagement <span>· Irrigation Control</span></h1>
  <div class="chips" id="chips"></div>
  <div id="conn">conectando…</div>
</header>
<main>
  <section class="card">
    <h2>Estaciones de riego</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>ID</th><th>Ubicación</th><th>Estado</th><th>Caudal (L/min)</th><th>Volumen (L)</th><th>Operario</th></tr></thead>
        <tbody id="tbody"><tr><td colspan="6">Cargando…</td></tr></tbody>
      </table>
    </div>
  </section>
  <div class="side">
    <section class="card">
      <h2>Control</h2>
      <div class="body">
        <div id="sel">Ninguna estación seleccionada</div>
        <div id="selinfo">Haz clic en una fila de la tabla</div>
        <div style="margin-top:12px">
          <label for="seg">Duración del riego (segundos)</label>
          <input id="seg" type="number" min="1" value="30">
        </div>
        <div class="row">
          <button class="go" id="bRiego">Iniciar riego</button>
          <button class="stop" id="bBloq">Bloquear</button>
          <button id="bAct">Activar</button>
        </div>
        <div class="row">
          <button class="all" id="bBloqAll">Bloquear todas</button>
          <button class="all" id="bActAll">Activar todas</button>
        </div>
        <div id="msg"></div>
      </div>
    </section>
    <section class="card">
      <h2>Mensajes de la aplicación</h2>
      <div id="log"></div>
    </section>
  </div>
</main>
<script>
  var seleccion = null;
  var token = '';
  try { token = sessionStorage.getItem('tok') || ''; } catch (e) {}
  var ETIQ = { AVAILABLE:'AVAILABLE', WATERING:'WATERING', LEAK:'LEAK / AVERÍA', OUT_OF_SERVICE:'FUERA DE SERVICIO', DISCONNECTED:'DISCONNECTED' };
  var COL = { AVAILABLE:'var(--ok)', WATERING:'var(--ok)', LEAK:'var(--leak)', OUT_OF_SERVICE:'var(--oos)', DISCONNECTED:'var(--off)' };
  var ORDEN = ['AVAILABLE','WATERING','LEAK','OUT_OF_SERVICE','DISCONNECTED'];

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
      return { '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;' }[c];
    });
  }

  function pintar(d) {
    var cuentas = {};
    ORDEN.forEach(function (k) { cuentas[k] = 0; });
    var html = '';
    d.estaciones.forEach(function (e) {
      var est = String(e.estado).toUpperCase();
      if (cuentas[est] !== undefined) cuentas[est]++;
      var cls = ETIQ[est] ? est : 'DISCONNECTED';
      var regando = est === 'WATERING';
      html += '<tr data-id="' + esc(e.id) + '"' + (e.id === seleccion ? ' class="sel"' : '') + '>' +
        '<td><b>' + esc(e.id) + '</b></td>' +
        '<td>' + esc(e.ubicacion) + '</td>' +
        '<td><span class="badge ' + cls + '">' + esc(ETIQ[est] || est) + '</span></td>' +
        '<td class="num">' + (regando ? e.caudal.toFixed(2) : '–') + '</td>' +
        '<td class="num">' + (regando || e.volumen > 0 ? e.volumen.toFixed(2) : '–') + '</td>' +
        '<td>' + esc(regando && e.operario ? e.operario : '–') + '</td></tr>';
    });
    document.getElementById('tbody').innerHTML = html || '<tr><td colspan="6">No hay estaciones registradas</td></tr>';

    var chips = '';
    ORDEN.forEach(function (k) {
      chips += '<span class="chip"><span class="dot" style="background:' + COL[k] + '"></span>' +
               cuentas[k] + ' ' + esc(ETIQ[k]) + '</span>';
    });
    document.getElementById('chips').innerHTML = chips;

    var log = '';
    d.eventos.forEach(function (ev) { log += '<div>' + esc(ev) + '</div>'; });
    document.getElementById('log').innerHTML = log || '<div>Sin eventos</div>';

    var s = d.estaciones.filter(function (e) { return e.id === seleccion; })[0];
    if (s) {
      document.getElementById('sel').textContent = s.id + ' · ' + s.ubicacion;
      document.getElementById('selinfo').textContent = 'Estado: ' + s.estado + (s.conectada ? ' (socket conectado)' : ' (sin socket)');
    }
  }

  function refrescar() {
    fetch('/api/estado', { cache: 'no-store' })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        pintar(d);
        var c = document.getElementById('conn');
        c.textContent = 'actualizado ' + new Date().toLocaleTimeString();
        c.className = '';
      })
      .catch(function () {
        var c = document.getElementById('conn');
        c.textContent = 'sin conexión con Central';
        c.className = 'bad';
      });
  }

  function mostrar(texto, ok) {
    var m = document.getElementById('msg');
    m.textContent = texto;
    m.className = ok ? 'ok' : 'err';
  }

  function accion(ruta, params, reintento) {
    var q = Object.keys(params).map(function (k) { return k + '=' + encodeURIComponent(params[k]); });
    if (token) q.push('token=' + encodeURIComponent(token));
    fetch(ruta + '?' + q.join('&'), { method: 'POST' })
      .then(function (r) {
        if (r.status === 403 && !reintento) {
          var t = prompt('Token del panel:');
          if (t) {
            token = t;
            try { sessionStorage.setItem('tok', t); } catch (e) {}
            accion(ruta, params, true);
          }
          return null;
        }
        return r.json();
      })
      .then(function (d) {
        if (!d) return;
        mostrar(d.ok ? 'Hecho' : (d.error || 'Error'), d.ok);
        refrescar();
      })
      .catch(function () { mostrar('No se pudo contactar con Central', false); });
  }

  function necesitaSeleccion() {
    if (!seleccion) { mostrar('Selecciona primero una estación', false); return false; }
    return true;
  }

  document.getElementById('tbody').addEventListener('click', function (ev) {
    var tr = ev.target.closest('tr');
    if (tr && tr.getAttribute('data-id')) {
      seleccion = tr.getAttribute('data-id');
      refrescar();
    }
  });
  document.getElementById('bRiego').onclick = function () {
    if (necesitaSeleccion()) accion('/api/riego', { id: seleccion, segundos: document.getElementById('seg').value });
  };
  document.getElementById('bBloq').onclick = function () {
    if (necesitaSeleccion()) accion('/api/bloquear', { id: seleccion });
  };
  document.getElementById('bAct').onclick = function () {
    if (necesitaSeleccion()) accion('/api/activar', { id: seleccion });
  };
  document.getElementById('bBloqAll').onclick = function () {
    if (confirm('¿Bloquear TODAS las estaciones?')) accion('/api/bloquear', { id: 'ALL' });
  };
  document.getElementById('bActAll').onclick = function () {
    if (confirm('¿Activar TODAS las estaciones bloqueadas?')) accion('/api/activar', { id: 'ALL' });
  };

  refrescar();
  setInterval(refrescar, 1000);
</script>
</body>
</html>
""";
}