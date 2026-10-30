"""Handle discover message for Home Assistant."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from functools import partial
import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from ..const import (
    ATTR_ADDON,
    ATTR_APP,
    ATTR_CONFIG,
    ATTR_DISCOVERY,
    FILE_HASSIO_DISCOVERY,
    AppState,
)
from ..coresys import CoreSys, CoreSysAttributes
from ..exceptions import HomeAssistantAPIError
from ..utils.common import FileConfiguration
from .validate import SCHEMA_DISCOVERY_CONFIG

if TYPE_CHECKING:
    from ..apps.app import App

_LOGGER: logging.Logger = logging.getLogger(__name__)

CMD_NEW = "post"
CMD_DEL = "delete"


@dataclass(slots=True)
class Message:
    """Represent a single Discovery message."""

    app: str
    service: str
    config: dict[str, Any] = field(compare=False)
    uuid: str = field(default_factory=lambda: uuid4().hex, compare=False)


@dataclass(slots=True, eq=False)
class _Mark:
    """Mark of a message that Home Assistant may have missed."""

    failed: bool = False


class Discovery(CoreSysAttributes, FileConfiguration):
    """Home Assistant Discovery handler."""

    def __init__(self, coresys: CoreSys):
        """Initialize discovery handler."""
        super().__init__(FILE_HASSIO_DISCOVERY, SCHEMA_DISCOVERY_CONFIG)
        self.coresys: CoreSys = coresys
        self.message_obj: dict[str, Message] = {}
        # Home Assistant may have missed these, so they are pushed again
        self._unannounced: dict[str, _Mark] = {}

    async def load(self) -> None:
        """Load exists discovery message into storage."""
        messages = {}
        for message in self._data[ATTR_DISCOVERY]:
            discovery = Message(**message)
            messages[discovery.uuid] = discovery

        _LOGGER.info("Loaded %d messages", len(messages))
        self.message_obj = messages
        # Supervisor does not know which of these Home Assistant has seen
        self._unannounced = {uuid: _Mark() for uuid in messages}

    async def save(self) -> None:
        """Write discovery message into data file."""
        messages: list[dict[str, Any]] = []
        for message in self.list_messages:
            messages.append(asdict(message))

        self._data[ATTR_DISCOVERY].clear()
        self._data[ATTR_DISCOVERY].extend(messages)
        await self.save_data()

    def get(self, uuid: str) -> Message | None:
        """Return discovery message."""
        return self.message_obj.get(uuid)

    @property
    def list_messages(self) -> list[Message]:
        """Return list of available discovery messages."""
        return list(self.message_obj.values())

    async def send(self, app: App, service: str, config: dict[str, Any]) -> Message:
        """Send a discovery message to Home Assistant."""
        # Create message
        message = Message(app.slug, service, config)

        # Already exists?
        for exists_msg in self.list_messages:
            if exists_msg != message:
                continue
            if exists_msg.config != config:
                message = exists_msg
                message.config = config
            elif exists_msg.uuid in self._unannounced:
                message = exists_msg
            else:
                _LOGGER.debug("Duplicate discovery message from %s", app.slug)
                return exists_msg
            break

        _LOGGER.info(
            "Sending discovery to Home Assistant %s from %s", service, app.slug
        )
        self.message_obj[message.uuid] = message
        await self.save()

        self._push(app, message)
        return message

    def mark_unannounced(self, app: App) -> None:
        """Push the messages of an app on their next send, even if unchanged."""
        for message in self.list_messages:
            if message.app == app.slug:
                self._unannounced[message.uuid] = _Mark()

    def push_messages(self, app: App) -> None:
        """Push all messages of an app, as Home Assistant may have missed them."""
        # New marks, since a push from startup may still be running
        self.mark_unannounced(app)
        for message in self.list_messages:
            if message.app == app.slug:
                self._push(app, message)

    def retry_failed_pushes(self) -> None:
        """Push the messages of started apps whose last push failed."""
        for message in self.list_messages:
            mark = self._unannounced.get(message.uuid)
            app = self.sys_apps.get_local_only(message.app)
            if mark and mark.failed and app and app.state == AppState.STARTED:
                self._push(app, message)

    def forget_failed_pushes(self, messages: list[Message]) -> None:
        """Forget the failed pushes of messages Home Assistant listed itself."""
        for message in messages:
            if mark := self._unannounced.get(message.uuid):
                mark.failed = False

    async def remove(self, message: Message) -> None:
        """Remove a discovery message from Home Assistant."""
        self.message_obj.pop(message.uuid, None)
        self._unannounced.pop(message.uuid, None)
        await self.save()

        _LOGGER.info(
            "Delete discovery to Home Assistant %s from %s",
            message.service,
            message.app,
        )
        self.sys_create_task(self._push_discovery(message, CMD_DEL))

    def _push(self, app: App, message: Message) -> None:
        """Push a message to Home Assistant."""
        mark = self._unannounced.get(message.uuid)
        push = self.sys_create_task(self._push_discovery(message, CMD_NEW))
        push.add_done_callback(partial(self._mark_announced, app, message, mark))

    def _mark_announced(
        self,
        app: App,
        message: Message,
        mark: _Mark | None,
        push: asyncio.Task[bool],
    ) -> None:
        """Clear the unannounced mark once Home Assistant got the message."""
        delivered = not push.cancelled() and push.result()
        # Keep the mark that a restart during the push set for the next start
        if not mark or self._unannounced.get(message.uuid) is not mark:
            return
        mark.failed = not delivered
        if delivered and app.state == AppState.STARTED:
            self._unannounced.pop(message.uuid, None)

    async def _push_discovery(self, message: Message, command: str) -> bool:
        """Send a discovery request and return whether Home Assistant got it."""
        if not await self.sys_homeassistant.api.check_api_state():
            _LOGGER.info("Discovery %s message ignore", message.uuid)
            return False

        data = asdict(message)
        data.pop(ATTR_CONFIG)
        # Home Assistant expects the legacy "addon" key in the push payload.
        data[ATTR_ADDON] = data.pop(ATTR_APP)

        try:
            async with self.sys_homeassistant.api.make_request(
                command,
                f"api/hassio_push/discovery/{message.uuid}",
                json=data,
                timeout=10,
            ) as resp:
                if resp.status in (200, 201):
                    _LOGGER.info("Discovery %s message send", message.uuid)
                    return True
                _LOGGER.warning(
                    "Discovery %s message failed with %i", message.uuid, resp.status
                )
        except HomeAssistantAPIError as err:
            _LOGGER.error("Discovery %s message failed: %s", message.uuid, err)
        return False
