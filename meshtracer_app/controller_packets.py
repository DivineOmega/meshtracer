from __future__ import annotations

import hashlib
from typing import Any

from .meshtastic_helpers import extract_node_position, node_record_from_node, node_record_from_num


class ControllerPacketMixin:
    @staticmethod
    def _telemetry_type(raw_type: Any) -> tuple[str, str] | None:
        text = str(raw_type or "").strip().lower().replace("-", "_").replace(" ", "_")
        mapping = {
            "device": ("device", "device_metrics"),
            "device_metrics": ("device", "device_metrics"),
            "environment": ("environment", "environment_metrics"),
            "environment_metrics": ("environment", "environment_metrics"),
            "power": ("power", "power_metrics"),
            "power_metrics": ("power", "power_metrics"),
        }
        return mapping.get(text)

    @staticmethod
    def _telemetry_packet_types(packet: Any) -> list[str]:
        if not isinstance(packet, dict):
            return []
        decoded = packet.get("decoded")
        telemetry = decoded.get("telemetry") if isinstance(decoded, dict) else None
        if not isinstance(telemetry, dict):
            return []

        telemetry_types: list[str] = []
        if isinstance(telemetry.get("deviceMetrics"), dict) or isinstance(
            telemetry.get("device_metrics"), dict
        ):
            telemetry_types.append("device")
        if isinstance(telemetry.get("environmentMetrics"), dict) or isinstance(
            telemetry.get("environment_metrics"), dict
        ):
            telemetry_types.append("environment")
        if isinstance(telemetry.get("powerMetrics"), dict) or isinstance(
            telemetry.get("power_metrics"), dict
        ):
            telemetry_types.append("power")
        return telemetry_types

    @staticmethod
    def _packet_decoded(packet: Any) -> dict[str, Any] | None:
        if not isinstance(packet, dict):
            return None
        decoded = packet.get("decoded")
        return decoded if isinstance(decoded, dict) else None

    @classmethod
    def _packet_portnum(cls, packet: Any) -> str:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return ""
        value = decoded.get("portnum")
        if value is None:
            value = decoded.get("portNum")
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            try:
                value_int = int(value)
            except (TypeError, ValueError):
                value_int = None
            if value_int is not None:
                if value_int == 1:
                    return "TEXT_MESSAGE_APP"
                if value_int == 7:
                    return "TEXT_MESSAGE_COMPRESSED_APP"
                if value_int == 3:
                    return "POSITION_APP"
                if value_int == 4:
                    return "NODEINFO_APP"
                if value_int == 67:
                    return "TELEMETRY_APP"
                return str(value_int)
        text = str(value).strip().upper()
        if text.isdigit():
            return cls._packet_portnum({"decoded": {"portnum": int(text)}})
        return text

    @classmethod
    def _is_node_info_packet(cls, packet: Any) -> bool:
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict):
            if isinstance(decoded.get("user"), dict):
                return True
            if isinstance(decoded.get("nodeInfo"), dict):
                return True
            if isinstance(decoded.get("node_info"), dict):
                return True
            if isinstance(decoded.get("nodeinfo"), dict):
                return True
        return cls._packet_portnum(packet) == "NODEINFO_APP"

    @classmethod
    def _is_position_packet(cls, packet: Any) -> bool:
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict) and isinstance(decoded.get("position"), dict):
            return True
        return cls._packet_portnum(packet) == "POSITION_APP"

    @classmethod
    def _packet_position(cls, packet: Any) -> tuple[float | None, float | None]:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return None, None
        position = decoded.get("position")
        if not isinstance(position, dict):
            return None, None
        return extract_node_position({"position": position})

    @staticmethod
    def _packet_int(packet: Any, key: str) -> int | None:
        if not isinstance(packet, dict):
            return None
        value = packet.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _packet_float(packet: Any, *keys: str) -> float | None:
        if not isinstance(packet, dict):
            return None
        for key in keys:
            if key not in packet:
                continue
            value = packet.get(key)
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @classmethod
    def _packet_hops_away(cls, packet: Any) -> int | None:
        if not isinstance(packet, dict):
            return None

        hops_away = cls._packet_int(packet, "hopsAway")
        if hops_away is None:
            hops_away = cls._packet_int(packet, "hops_away")
        if hops_away is not None:
            return hops_away if hops_away >= 0 else None

        hop_start = cls._packet_int(packet, "hopStart")
        if hop_start is None:
            hop_start = cls._packet_int(packet, "hop_start")
        hop_limit = cls._packet_int(packet, "hopLimit")
        if hop_limit is None:
            hop_limit = cls._packet_int(packet, "hop_limit")
        if hop_start is None or hop_limit is None:
            return None
        if hop_limit < 0 or hop_start < hop_limit:
            return None
        return hop_start - hop_limit

    @staticmethod
    def _interface_local_node_num(interface: Any) -> int | None:
        local_num = getattr(getattr(interface, "localNode", None), "nodeNum", None)
        try:
            return int(local_num) if local_num is not None else None
        except (TypeError, ValueError):
            return None

    @classmethod
    def _is_text_message_packet(cls, packet: Any) -> bool:
        portnum = cls._packet_portnum(packet)
        return portnum in ("TEXT_MESSAGE_APP", "TEXT_MESSAGE_COMPRESSED_APP")

    @classmethod
    def _packet_text(cls, packet: Any) -> str | None:
        decoded = cls._packet_decoded(packet)
        if not isinstance(decoded, dict):
            return None
        text_value = decoded.get("text")
        if isinstance(text_value, str):
            text = text_value.strip()
            return text or None

        payload = decoded.get("payload")
        if isinstance(payload, (bytes, bytearray)):
            try:
                text = bytes(payload).decode("utf-8").strip()
            except Exception:
                return None
            return text or None
        return None

    @staticmethod
    def _is_broadcast_node_num(node_num: int | None) -> bool:
        if node_num is None:
            return False
        return node_num in (-1, 0xFFFFFFFF)

    @classmethod
    def _is_broadcast_packet_destination(cls, packet: Any) -> bool:
        to_num = cls._packet_int(packet, "to")
        if cls._is_broadcast_node_num(to_num):
            return True
        if not isinstance(packet, dict):
            return False
        to_id = str(packet.get("toId") or "").strip().lower()
        if to_id in ("^all", "all", "broadcast", "!ffffffff"):
            return True
        return False

    @classmethod
    def _dedupe_key_for_chat_packet(
        cls,
        *,
        packet_id: int | None,
        from_node_num: int | None,
        to_node_num: int | None,
        message_type: str,
        channel_index: int | None,
        peer_node_num: int | None,
        rx_time: float | None,
        text: str,
    ) -> str:
        normalized_to_node = to_node_num
        if cls._is_broadcast_node_num(normalized_to_node):
            normalized_to_node = 0xFFFFFFFF
        if packet_id is not None:
            scope = f"c{channel_index}" if message_type == "channel" else f"p{peer_node_num}"
            return f"pkt:{packet_id}:{from_node_num}:{normalized_to_node}:{scope}"
        rx_stamp = f"{rx_time:.3f}" if isinstance(rx_time, float) else "-"
        text_hash = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]
        scope = f"c{channel_index}" if message_type == "channel" else f"p{peer_node_num}"
        return f"rt:{rx_stamp}:{from_node_num}:{normalized_to_node}:{scope}:{text_hash}"

    @staticmethod
    def _channel_name_text(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        return text

    @staticmethod
    def _field_value(obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    @staticmethod
    def _modem_preset_label(value: Any) -> str | None:
        by_number = {
            0: "LongFast",
            1: "LongSlow",
            2: "VeryLongSlow",
            3: "MediumSlow",
            4: "MediumFast",
            5: "ShortSlow",
            6: "ShortFast",
            7: "LongModerate",
            8: "ShortTurbo",
            9: "LongTurbo",
        }
        try:
            preset_num = int(value)
        except (TypeError, ValueError):
            preset_num = None
        if preset_num is not None and preset_num in by_number:
            return by_number[preset_num]

        text = str(value or "").strip().upper()
        if not text:
            return None
        if "." in text:
            text = text.split(".")[-1]
        if "_" in text:
            parts = [part for part in text.split("_") if part]
            if parts:
                return "".join(part[:1].upper() + part[1:].lower() for part in parts)
        return None

    @classmethod
    def _interface_primary_channel_label(cls, interface: Any) -> str | None:
        local_node = getattr(interface, "localNode", None)
        local_config = cls._field_value(local_node, "localConfig")
        lora = cls._field_value(local_config, "lora")
        preset_val = cls._field_value(lora, "modem_preset")
        if preset_val is None:
            preset_val = cls._field_value(lora, "modemPreset")
        return cls._modem_preset_label(preset_val)

    @staticmethod
    def _channel_role_text(value: Any) -> str:
        try:
            role_num = int(value)
        except (TypeError, ValueError):
            role_num = None
        if role_num is not None:
            if role_num == 0:
                return "DISABLED"
            if role_num == 1:
                return "PRIMARY"
            if role_num == 2:
                return "SECONDARY"
            if role_num == 3:
                return "ADMIN"
        text = str(value or "").strip().upper()
        if not text:
            return ""
        if "." in text:
            text = text.split(".")[-1]
        return text

    @staticmethod
    def _channel_role_label(role_text: str, channel_index: int) -> str | None:
        role_upper = str(role_text or "").strip().upper()
        if role_upper == "PRIMARY":
            return "Primary"
        if role_upper == "SECONDARY":
            return f"Secondary {channel_index}" if channel_index > 0 else "Secondary"
        if role_upper == "ADMIN":
            return "Admin"
        return None

    @classmethod
    def _interface_channel_indexes_and_names(cls, interface: Any) -> tuple[list[int], dict[int, str]]:
        primary_channel_label = cls._interface_primary_channel_label(interface) or "Primary"
        channels_obj = getattr(getattr(interface, "localNode", None), "channels", None)
        if channels_obj is None:
            return [0], {0: primary_channel_label}
        try:
            channels = list(channels_obj)
        except TypeError:
            return [0], {0: primary_channel_label}

        values: list[int] = []
        names: dict[int, str] = {}
        for channel in channels:
            role_val: Any = None
            index_val: Any = None
            name_val: Any = None
            settings_val: Any = None
            if isinstance(channel, dict):
                role_val = channel.get("role")
                index_val = channel.get("index")
                name_val = channel.get("name")
                settings_val = channel.get("settings")
            else:
                role_val = getattr(channel, "role", None)
                index_val = getattr(channel, "index", None)
                name_val = getattr(channel, "name", None)
                settings_val = getattr(channel, "settings", None)

            role_text = cls._channel_role_text(role_val)
            if role_val == 0 or role_text == "DISABLED":
                continue
            try:
                idx = int(index_val)
            except (TypeError, ValueError):
                continue
            if idx < 0:
                continue
            values.append(idx)
            name_text = cls._channel_name_text(name_val)
            if name_text is None:
                if isinstance(settings_val, dict):
                    name_text = cls._channel_name_text(settings_val.get("name"))
                elif settings_val is not None:
                    name_text = cls._channel_name_text(getattr(settings_val, "name", None))
            if name_text is None:
                if idx == 0:
                    name_text = primary_channel_label
                else:
                    name_text = cls._channel_role_label(role_text, idx)
            if name_text is not None and idx not in names:
                names[idx] = name_text

        if 0 not in values:
            values.insert(0, 0)
        if 0 in values and 0 not in names:
            names[0] = primary_channel_label
        return sorted(set(values)), names

    @classmethod
    def _interface_channel_indexes(cls, interface: Any) -> list[int]:
        values, _names = cls._interface_channel_indexes_and_names(interface)
        return values

    @staticmethod
    def _node_log_descriptor_from_record(node_num: int, record: Any) -> str:
        long_name = ""
        short_name = ""
        if isinstance(record, dict):
            long_name = str(record.get("long_name") or "").strip()
            short_name = str(record.get("short_name") or "").strip()
        if not long_name:
            long_name = "-"
        if not short_name:
            short_name = "-"
        long_name = long_name.replace('"', "'")
        short_name = short_name.replace('"', "'")
        return f'node #{node_num} (long="{long_name}", short="{short_name}")'

    @classmethod
    def _node_log_descriptor(cls, interface: Any, node_num: Any, packet: Any = None) -> str:
        try:
            node_num_int = int(node_num)
        except (TypeError, ValueError):
            return "node #?"

        packet_record: dict[str, Any] | None = None
        decoded = cls._packet_decoded(packet)
        if isinstance(decoded, dict):
            user = decoded.get("user")
            if isinstance(user, dict):
                packet_record = node_record_from_node({"num": node_num_int, "user": user})
                packet_record["num"] = node_num_int

        if packet_record is not None:
            return cls._node_log_descriptor_from_record(node_num_int, packet_record)

        try:
            record = node_record_from_num(interface, node_num_int)
        except Exception:
            record = {"num": node_num_int}
        return cls._node_log_descriptor_from_record(node_num_int, record)
