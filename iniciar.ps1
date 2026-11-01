param([Parameter(Mandatory=$true, ValueFromRemainingArguments=$true)][string[]]$servicios)

$env:COMPOSE_PROFILES = "operarios,estaciones"   # Solo hace falta si mantienes los profiles
docker compose up -d @servicios

foreach ($s in $servicios) {
  if ($s -match "monitor") { $cmd = "docker compose logs -f --tail 50 $s" }
  else                     { $cmd = "docker compose attach $s" }
  Start-Process powershell -WorkingDirectory $PWD -ArgumentList "-NoExit", "-Command", "`$env:COMPOSE_PROFILES='operarios,estaciones'; `$Host.UI.RawUI.WindowTitle='$s'; $cmd"
}