import jwt
from fastapi import Header

from . import config
from .errors import ApiError


class InvalidToken(Exception):
    pass


def decode_user_id(token: str) -> int:
    """Verify a Django simplejwt access token locally (no DB call) and return its user_id."""
    try:
        payload = jwt.decode(
            token,
            config.JWT_SIGNING_KEY,
            algorithms=["HS256"],
            options={"require": ["exp"]},
        )
        if payload.get("token_type", "access") != "access":  # a refresh token must not buy
            raise InvalidToken("not an access token")
        return int(payload["user_id"])
    except (jwt.PyJWTError, KeyError, ValueError, TypeError) as exc:
        raise InvalidToken(str(exc)) from exc


async def current_user(authorization: str | None = Header(default=None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise ApiError(401, "unauthorized")
    try:
        return decode_user_id(authorization[len("Bearer "):])
    except InvalidToken:
        raise ApiError(401, "unauthorized") from None
