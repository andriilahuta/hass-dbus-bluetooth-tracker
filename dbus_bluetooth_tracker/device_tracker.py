from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import voluptuous as vol
from dbus_fast import BusType, Message, Variant, MessageType
from dbus_fast.aio import MessageBus

import homeassistant.helpers.config_validation as cv
import homeassistant.util.dt as dt_util
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
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType


logger = logging.getLogger(__name__)

BT_PREFIX = 'BT_'

BLUEZ_PATH = '/org/bluez'
BLUEZ_SERVICE = 'org.bluez'
ADAPTER_INTERFACE = f'{BLUEZ_SERVICE}.Adapter1'
DEVICE_INTERFACE = f'{BLUEZ_SERVICE}.Device1'

CONF_SEEN_SCAN_INTERVAL = 'seen_interval_seconds'
SEEN_SCAN_INTERVAL = timedelta()

PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA.extend(
    {
        vol.Optional(CONF_SCAN_INTERVAL, default=SCAN_INTERVAL): cv.time_period,
        vol.Optional(CONF_SEEN_SCAN_INTERVAL, default=SEEN_SCAN_INTERVAL): cv.time_period,
        vol.Optional(CONF_CONSIDER_HOME, default=DEFAULT_CONSIDER_HOME): vol.All(
            cv.time_period, cv.positive_timedelta
        ),
        vol.Optional(CONF_NEW_DEVICE_DEFAULTS, default={}): NEW_DEVICE_DEFAULTS_SCHEMA,
    }
)


class BtDeviceTracker:
    connect_timeout = 5

    def __init__(self, bus: MessageBus, adapter: str, mac: str):
        self._bus = bus
        self._mac = mac

        self._adapter_path = f'{BLUEZ_PATH}/{adapter}'
        self._device_path = f'{self._adapter_path}/dev_{mac.replace(":", "_")}'

    async def ping(self) -> bool:
        logger.debug('Pinging %s', self._mac)
        try:
            return await self._connect()
        finally:
            await self._disconnect()

    async def _connect(self) -> bool:
        try:
            async with asyncio.timeout(self.connect_timeout):
                res = await self._bus.call(Message(
                    destination=BLUEZ_SERVICE, interface=ADAPTER_INTERFACE, path=self._adapter_path,
                    member='ConnectDevice', signature='a{sv}', body=[{'Address': Variant('s', self._mac)}],
                ))
        except asyncio.TimeoutError:
            return False

        if res.message_type == MessageType.METHOD_RETURN:
            if (res_device_path := next(iter(res.body), '')) != self._device_path:
                logger.warning('Unexpected device path, expected: %s, got: %s', self._device_path, res_device_path)
            return True

        if res.message_type == MessageType.ERROR:
            if res.error_name == 'org.freedesktop.DBus.Error.UnknownMethod':
                logger.error('; '.join(res.body))
            if res.error_name == f'{BLUEZ_SERVICE}.Error.AlreadyExists':
                logger.info('Device %s already exists, reconnecting', self._device_path)
                await self._disconnect()
                await asyncio.sleep(1)
                return await self._connect()
            return False

        return False

    async def _disconnect(self) -> bool:
        await self._bus.call(Message(
            destination=BLUEZ_SERVICE, interface=DEVICE_INTERFACE, path=self._device_path,
            member='Disconnect',
        ))
        res = await self._bus.call(Message(
            destination=BLUEZ_SERVICE, interface=ADAPTER_INTERFACE, path=self._adapter_path,
            member='RemoveDevice', signature='o', body=[self._device_path],
        ))
        return res.message_type == MessageType.METHOD_RETURN


def is_bluetooth_device(device: Device) -> bool:
    """Check whether a device is a bluetooth device by its mac."""
    return device.mac is not None and device.mac[:3].upper() == BT_PREFIX


async def get_tracking_devices(hass: HomeAssistant) -> tuple[dict[str, str], dict[str, str]]:
    """Load all known devices."""
    yaml_path = hass.config.path(YAML_DEVICES)

    devices = await async_load_config(yaml_path, hass, timedelta(0))
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

    return devices_to_track, devices_to_not_track


async def see_device(hass: HomeAssistant, async_see: AsyncSeeCallback, mac: str, device_name: str):
    """Mark a device as seen."""
    logger.debug('Seen device: %s', mac)
    await async_see(mac=BT_PREFIX + mac, host_name=device_name, source_type=SourceType.BLUETOOTH)


async def async_setup_scanner(
        hass: HomeAssistant,
        config: ConfigType,
        async_see: AsyncSeeCallback,
        discovery_info: DiscoveryInfoType | None = None,
) -> bool:
    """Set up the Bluetooth Scanner."""
    interval: timedelta = config[CONF_SCAN_INTERVAL]
    seen_interval: timedelta = config[CONF_SEEN_SCAN_INTERVAL]
    update_bluetooth_lock = asyncio.Lock()

    devices_to_track, _ = await get_tracking_devices(hass)
    if not devices_to_track:
        logger.warning('No Bluetooth devices to track')

    seen_devices: dict[str, datetime] = {}

    async def perform_bluetooth_update():
        """Discover Bluetooth devices and update status."""
        logger.debug('Performing Bluetooth devices update')
        adapters = await bluetooth_api._get_manager(hass).async_get_bluetooth_adapters()
        bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        now = dt_util.utcnow()

        def _is_recently_seen(mac: str) -> bool:
            if mac not in seen_devices:
                return False
            if seen_interval == timedelta():
                return seen_devices[mac] == now
            return now - seen_devices[mac] <= seen_interval

        try:
            for adapter in adapters:
                for mac in devices_to_track:
                    if _is_recently_seen(mac):
                        continue
                    if await BtDeviceTracker(bus, adapter, mac).ping():
                        seen_devices[mac] = now
        finally:
            bus.disconnect()
            await bus.wait_for_disconnect()

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

    cancels = [
        async_track_time_interval(hass, update_bluetooth, interval),
    ]

    @callback
    def _async_handle_stop(event: Event):
        for cancel in cancels:
            cancel()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_handle_stop)
    hass.async_create_task(update_bluetooth())

    return True
