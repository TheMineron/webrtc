import logging
from functools import wraps
from typing import Callable, Coroutine, Any, TypeVar, ParamSpec

from p2p.utils import CommandContext, RoomParticipantPair

logger = logging.getLogger(__name__)

Handler = Callable[[CommandContext, dict], Coroutine[Any, Any, RoomParticipantPair]]
_COMMAND_HANDLERS: dict[str, Handler] = {}

P = ParamSpec('P')
R = TypeVar('R', bound=Coroutine[Any, Any, RoomParticipantPair])


def register_handler(
        command: str,
        *,
        overwrite: bool = False
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(handler: Callable[P, R]) -> Callable[P, R]:
        @wraps(handler)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return handler(*args, **kwargs)

        if command in _COMMAND_HANDLERS and not overwrite:
            logger.warning(
                f"Обработчик для команды '{command}' уже существует. "
                "Используйте overwrite=True для перезаписи."
            )
        else:
            _COMMAND_HANDLERS[command] = handler  # type: ignore[assignment]
            logger.debug(f"Зарегистрирован обработчик для команды '{command}'")

        return wrapper

    return decorator


def get_commands_handlers() -> dict[str, Handler]:
    return dict(_COMMAND_HANDLERS)
