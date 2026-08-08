

#### Esta es una prueba de concepto.

#### Ejemplo de configuración:
```yaml
device_tracker:
  - platform: dbus_bluetooth_tracker
    interval_seconds: 15
    # Consultar con menos frecuencia cuando el dispositivo ya haya sido visto recientemente
    seen_interval_seconds: 60
    consider_home: 90
    device_connect_timeout: 5
```

#### Advertencias:
* La detección automática no está implementada, por lo que los dispositivos rastreables ya deben estar presentes en `known_devices.yaml`
* Las funciones experimentales de BlueZ deben estar habilitadas (bandera `--experimental`)
