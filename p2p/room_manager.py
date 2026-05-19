import logging
from database.crud.rooms import (
    create_room,
    get_room_params,
    set_room_locked, set_room_max_participants,
    set_room_password_hash, set_room_moderator,
)
from database.crud.participants import (
    add_participant_to_room,
    remove_participant_from_room,
    set_participant_role,
    update_participant_nickname,
)
from database.models import Room, Participant

logger = logging.getLogger(__name__)


class RoomManager:
    def __init__(self):
        self.rooms: dict[str, Room] = {}

    async def get_or_create(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            params = await get_room_params(room_id)
            room = Room(room_id)
            room.is_locked = params["is_locked"]
            room.max_participants = params["max_participants"]
            room.password_hash = params["password_hash"]
            room.moderator_id = params["moderator_id"]
            self.rooms[room_id] = room
            await create_room(room_id)
            logger.info(f"Создана комната {room_id}")
        return self.rooms[room_id]

    async def add_participant(self, room: Room, participant: Participant) -> None:
        if room.moderator_id is None:
            participant.role = "moderator"
            room.moderator_id = participant.id
            await set_room_moderator(room.id, participant.id)
        room.participants[participant.id] = participant
        await add_participant_to_room(room.id, participant.id, participant.nickname)
        await set_participant_role(participant.id, participant.role)

    async def remove_participant(
            self,
            room: Room,
            participant_id: str
    ) -> Participant | None:
        participant = room.participants.pop(participant_id, None)
        if participant:
            await remove_participant_from_room(room.id, participant_id)
            if participant_id == room.moderator_id and room.participants:
                new_moderator = next(iter(room.participants.values()))
                room.moderator_id = new_moderator.id
                new_moderator.role = "moderator"
                await set_room_moderator(room.id, new_moderator.id)
                await set_participant_role(new_moderator.id, "moderator")
        return participant

    async def remove_if_empty(self, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room and not room.participants:
            del self.rooms[room_id]
            from database.crud.rooms import delete_room
            await delete_room(room_id)
            logger.info(f"Комната {room_id} удалена (пуста)")

    async def update_room_params(self, room: Room) -> None:
        await set_room_locked(room.id, room.is_locked)
        await set_room_max_participants(room.id, room.max_participants)
        await set_room_password_hash(room.id, room.password_hash)
        await set_room_moderator(room.id, room.moderator_id)

    async def update_participant_nickname(
            self,
            room: Room,
            participant_id: str,
            new_nickname: str
    ) -> None:
        participant = room.participants.get(participant_id)
        if not participant:
            return
        old_nickname = participant.nickname
        participant.nickname = new_nickname
        await update_participant_nickname(participant_id, new_nickname)
        logger.debug(
            f"Никнейм участника {participant_id} "
            f"изменён: {old_nickname} -> {new_nickname}"
        )
