from typing import Optional, List, Dict
from dataclasses import dataclass, field
import secrets
import hashlib

from fastapi import WebSocket


@dataclass
class Room:
    id: str
    is_locked: bool = False
    max_participants: Optional[int] = None
    password_hash: Optional[str] = None
    moderator_id: Optional[str] = None
    participants: Dict[str, 'Participant'] = field(default_factory=dict)

    def set_password(self, password: Optional[str]):
        if password:
            self.password_hash = hashlib.sha256(password.encode()).hexdigest()
        else:
            self.password_hash = None

    def check_password(self, password: str) -> bool:
        if not self.password_hash:
            return True
        return self.password_hash == hashlib.sha256(password.encode()).hexdigest()

    def can_join(self, password: Optional[str] = None) -> tuple[bool, str]:
        if self.is_locked:
            return False, "Комната закрыта для входа"
        if self.max_participants and len(self.participants) >= self.max_participants:
            return False, "Достигнуто максимальное количество участников"
        if not self.check_password(password or ""):
            return False, "Неверный пароль"
        return True, ""

    def get_info_list(self, exclude_id: Optional[str] = None) -> List[dict]:
        return [
            {
                "id": p.id,
                "name": p.nickname,
                "role": p.role,
                "audio_enabled": p.audio_enabled,
                "video_enabled": p.video_enabled
            }
            for p in self.participants.values()
            if p.id != exclude_id
        ]


@dataclass
class Participant:
    websocket: WebSocket
    id: str = field(default_factory=lambda: secrets.token_urlsafe(16))
    nickname: str = ""
    role: str = "participant"
    audio_enabled: bool = True
    video_enabled: bool = True
    screen_sharing: bool = False

    def __post_init__(self):
        if not self.nickname:
            self.nickname = f"User_{self.id[:6]}"


@dataclass
class ChatMessage:
    msg_id: str
    from_id: str
    from_name: str
    text: str
    target_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: __import__('time').time())
    edited: bool = False
    deleted: bool = False
    is_pinned: bool = False
    reply_to_msg_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "msg_id": self.msg_id,
            "from_id": self.from_id,
            "from_name": self.from_name,
            "text": self.text,
            "target_id": self.target_id,
            "timestamp": self.timestamp,
            "edited": self.edited,
            "deleted": self.deleted,
            "is_pinned": self.is_pinned,
            "reply_to_msg_id": self.reply_to_msg_id
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ChatMessage':
        return cls(**data)
