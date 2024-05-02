#### This is a proof of concept.

#### Config example:
```yaml
device_tracker:
  - platform: dbus_bluetooth_tracker
    interval_seconds: 15
    # Poll less frequently when the device has been already seen recently
    seen_interval_seconds: 60
    consider_home: 90
```

#### Caveats:
* Discovery is not implemented, so trackable devices should already be present in `known_devices.yaml`
* Bluez experimental features should be enabled (`--experimental` flag)
