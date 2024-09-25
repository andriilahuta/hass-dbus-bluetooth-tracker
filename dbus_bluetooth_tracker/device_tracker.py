from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta

import voluptuous as vol
from dbus_fast import BusType, Message, Variant, MessageType
from dbus_fast.aio import MessageBus

import homeassistant.helpers.config_validation as cv
import homeassistant.util.dt as dt_util
from habluetooth import HaScanner
from homeassistant.components.bluetooth import api as bluetooth_api
from homeassistant.components.device_tracker import (
    CONF_CONSIDER_HOME,
    CONF_NEW_DEVICE_DEFAULTS,
    CONF_SCAN_INTERVAL,
    DEFAULT_CONSIDER_HOME,
    SCAN_INTERVAL,
    SourceType,
)
from homeassistant.components.device_tracker.legacy import (
    NEW_DEVICE_DEFAULTS_SCHEMA,
    YAML_DEVICES,
    AsyncSeeCallback,
    Device,
    async_load_config,
)
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant, callback, ServiceCall
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType


logger = logging.getLogger(__name__)

BT_PREFIX = 'BT_'

BLUEZ_PATH = '/org/bluez'
BLUEZ_SERVICE = 'org.bluez'
ADAPTER_INTERFACE = f'{BLUEZ_SERVICE}.Adapter1'
DEVICE_INTERFACE = f'{BLUEZ_SERVICE}.Device1'

DOMAIN = 'dbus_bluetooth_tracker'
SERVICE_INVALIDATE_BLUETOOTH_CACHE = 'invalidate_bluetooth_cache'

CONF_SEEN_SCAN_INTERVAL = 'seen_interval_seconds'
CONF_DEVICE_CONNECT_TIMEOUT = 'device_connect_timeout'

SEEN_SCAN_INTERVAL = timedelta()
DEVICE_CONNECT_TIMEOUT = timedelta(seconds=5)

PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA.extend(
    {
        vol.Optional(CONF_SCAN_INTERVAL, default=SCAN_INTERVAL): cv.time_period,
        vol.Optional(CONF_SEEN_SCAN_INTERVAL, default=SEEN_SCAN_INTERVAL): cv.time_period,
        vol.Optional(CONF_DEVICE_CONNECT_TIMEOUT, default=DEVICE_CONNECT_TIMEOUT): cv.time_period,
        vol.Optional(CONF_CONSIDER_HOME, default=DEFAULT_CONSIDER_HOME): vol.All(
            cv.time_period, cv.positive_timedelta
        ),
        vol.Optional(CONF_NEW_DEVICE_DEFAULTS, default={}): NEW_DEVICE_DEFAULTS_SCHEMA,
    }
)


class BtDeviceTracker:
    def __init__(self, bus: MessageBus, adapter: str, mac: str, connect_timeout: timedelta = DEVICE_CONNECT_TIMEOUT):
        self._bus = bus
        self._mac = mac
        self._connect_timeout = connect_timeout

        self._adapter_path = f'{BLUEZ_PATH}/{adapter}'
        self._device_path = f'{self._adapter_path}/dev_{mac.replace(":", "_")}'

    async def ping(self) -> bool:
        logger.debug('Pinging %s with %s', self._mac, self._adapter_path)
        try:
            return await self._connect()
        finally:
            await self._disconnect()

    async def _connect(self) -> bool:
        try:
            async with asyncio.timeout(self._connect_timeout.seconds):
                res = await self._bus.call(Message(
                    destination=BLUEZ_SERVICE, interface=ADAPTER_INTERFACE, path=self._adapter_path,
                    member='ConnectDevice', signature='a{sv}', body=[{'Address': Variant('s', self._mac)}],
                ))
        except asyncio.TimeoutError:
            logger.debug('Timeout connecting to %s with %s', self._mac, self._adapter_path)
            return False

        if res.message_type == MessageType.METHOD_RETURN:
            if (res_device_path := next(iter(res.body), '')) != self._device_path:
                logger.warning('Unexpected device path, expected: %s, got: %s', self._device_path, res_device_path)
            return True

        if res.message_type == MessageType.ERROR:
            if res.error_name == 'org.freedesktop.DBus.Error.UnknownMethod':
                logger.error('; '.join(res.body))
            elif res.error_name == f'{BLUEZ_SERVICE}.Error.AlreadyExists':
                logger.info('Device %s already exists, reconnecting', self._device_path)
                await self._disconnect()
                await asyncio.sleep(1)
                return await self._connect()
            else:
                body = ', '.join(res.body) or '<empty>'
                logger.error('Failed connecting to %s with %s: %s, %s',
                             self._mac, self._adapter_path, res.error_name, body)
            return False

        return False

    async def _disconnect(self) -> bool:
        try:
            async with asyncio.timeout(self._connect_timeout.seconds):
                await self._bus.call(Message(
                    destination=BLUEZ_SERVICE, interface=DEVICE_INTERFACE, path=self._device_path,
                    member='Disconnect',
                ))
                res = await self._bus.call(Message(
                    destination=BLUEZ_SERVICE, interface=ADAPTER_INTERFACE, path=self._adapter_path,
                    member='RemoveDevice', signature='o', body=[self._device_path],
                ))
                return res.message_type == MessageType.METHOD_RETURN
        except asyncio.TimeoutError:
            logger.debug('Timeout disconnecting from %s with %s', self._mac, self._adapter_path)
            return False


def is_bluetooth_device(device: Device) -> bool:
    """Check whether a device is a bluetooth device by its mac."""
    return device.mac is not None and device.mac[:3].upper() == BT_PREFIX


async def get_tracking_devices(hass: HomeAssistant) -> tuple[dict[str, str], dict[str, str]]:
    """Load all known devices."""
    yaml_path = hass.config.path(YAML_DEVICES)
    logger.debug('Loading devices from %s', yaml_path)

    devices = await async_load_config(yaml_path, hass, timedelta(0))
    logger.debug('Loaded devices: %s', [device.mac for device in devices if devices])
    bluetooth_devices = [device for device in devices if is_bluetooth_device(device)]

    devices_to_track: dict[str, str] = {
        device.mac[3:]: device.name
        for device in bluetooth_devices
        if device.track and device.mac is not None
    }
    devices_to_not_track: dict[str, str] = {
        device.mac[3:]: device.name
        for device in bluetooth_devices
        if not device.track and device.mac is not None
    }

    logger.debug('devices_to_track: %s', list(devices_to_track))
    logger.debug('devices_to_not_track: %s', list(devices_to_not_track))
    return devices_to_track, devices_to_not_track


async def see_device(hass: HomeAssistant, async_see: AsyncSeeCallback, mac: str, device_name: str):
    """Mark a device as seen."""
    logger.debug('Seen device: %s', mac)
    await async_see(mac=BT_PREFIX + mac, host_name=device_name, source_type=SourceType.BLUETOOTH)


def get_bluetooth_scanners(hass: HomeAssistant) -> set[HaScanner]:
    return {
        scanner
        for scanner in bluetooth_api._get_manager(hass)._connectable_scanners
        if isinstance(scanner, HaScanner)
    }


async def async_setup_scanner(
        hass: HomeAssistant,
        config: ConfigType,
        async_see: AsyncSeeCallback,
        discovery_info: DiscoveryInfoType | None = None,
) -> bool:
    """Set up the Bluetooth Scanner."""
    interval: timedelta = config[CONF_SCAN_INTERVAL]
    seen_interval: timedelta = config[CONF_SEEN_SCAN_INTERVAL]
    connect_timeout: timedelta = config[CONF_DEVICE_CONNECT_TIMEOUT]
    update_bluetooth_lock = asyncio.Lock()

    devices_to_track, _ = await get_tracking_devices(hass)
    if not devices_to_track:
        logger.warning('No Bluetooth devices to track')

    seen_devices: dict[str, datetime] = {}

    async def perform_bluetooth_update():
        """Discover Bluetooth devices and update status."""
        logger.debug('Performing Bluetooth devices update')

        scanners = get_bluetooth_scanners(hass)
        if not scanners:
            logger.warning('No Bluetooth scanners found')
            return
        logger.debug('Using Bluetooth adapters: %s', [scanner.adapter for scanner in scanners])

        bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        logger.debug('D-Bus: connected: %s, name: %s', bus.connected, bus.unique_name)

        now = dt_util.utcnow()

        def _is_recently_seen(mac: str) -> bool:
            if mac not in seen_devices:
                return False
            if seen_interval == timedelta():
                return seen_devices[mac] == now
            return now - seen_devices[mac] <= seen_interval

        try:
            for scanner in scanners:
                for mac in devices_to_track:
                    if _is_recently_seen(mac):
                        continue
                    if await BtDeviceTracker(bus, scanner.adapter, mac, connect_timeout).ping():
                        seen_devices[mac] = now
        finally:
            bus.disconnect()
            logger.debug('Disconnecting from D-Bus')
            try:
                async with asyncio.timeout(connect_timeout.seconds):
                    await bus.wait_for_disconnect()
            except asyncio.TimeoutError:
                logger.error('Timeout disconnecting from D-Bus')

        logger.debug('Seen devices @ %s: %s', now, {k: v.isoformat() for k, v in seen_devices.items()})
        for mac in seen_devices:
            if _is_recently_seen(mac):
                name = devices_to_track.get(mac, mac)
                hass.async_create_task(see_device(hass, async_see, mac, name))

    async def update_bluetooth(now: datetime | None = None):
        """Lookup Bluetooth devices and update status."""
        # If an update is in progress, we don't do anything
        if update_bluetooth_lock.locked():
            logger.debug(
                'Previous execution of update_bluetooth is taking longer than the scheduled update of interval %s',
                interval,
            )
            return

        async with update_bluetooth_lock:
            await perform_bluetooth_update()

    async def invalidate_bluetooth_cache(service: ServiceCall):
        logger.error('Not implemented')

    cancels: set[Callable[[], None]] = set()

    def _async_handle_start():
        logger.debug('Starting Bluetooth tasks')
        cancels.add(async_track_time_interval(hass, update_bluetooth, interval))

    @callback
    def _async_handle_stop(event: Event):
        logger.debug('Stopping Bluetooth tasks')
        while cancels:
            cancel = cancels.pop()
            cancel()

    _async_handle_start()
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_handle_stop)
    hass.async_create_task(update_bluetooth())
    hass.services.async_register(DOMAIN, SERVICE_INVALIDATE_BLUETOOTH_CACHE, invalidate_bluetooth_cache)

    return True
