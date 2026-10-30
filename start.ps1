# Abre las terminales de los módulos
$dir = $PSScriptRoot
Set-Location $dir

# Limpia restos de ejecuciones anteriores y construye la imagen una sola vez
docker compose down --remove-orphans
docker compose build

function Abrir($titulo, $comando) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "`$Host.UI.RawUI.WindowTitle='$titulo'; Set-Location '$dir'; $comando"
}

# 1) Monitores (solo muestran logs). Arrancan primero.
Abrir "MONITOR WS-01" "docker compose up ws1-monitor"
Abrir "MONITOR WS-02" "docker compose up ws2-monitor"

# 2) Engines: esperan 8 s a que el Monitor abra su socket (el Engine no reintenta)
Abrir "ENGINE WS-01" "Start-Sleep 8; docker compose run --rm --no-deps ws1-engine"
Abrir "ENGINE WS-02" "Start-Sleep 8; docker compose run --rm --no-deps ws2-engine"

# 3) Operarios
Abrir "FO-01" "docker compose run --rm --no-deps fo1"
Abrir "FO-02" "docker compose run --rm --no-deps fo2"