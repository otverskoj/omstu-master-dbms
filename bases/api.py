import typing

import fastapi
import psycopg
import pydantic

from dependencies import get_connection


__all__ = ['router']


router = fastapi.APIRouter(
    prefix='/bases',
    tags=['bases'],
)


class Base(pydantic.BaseModel):
    id: pydantic.UUID4
    name: str = pydantic.Field(..., min_length=1)
    active: bool
    description: str | None = None


class BaseToCreate(pydantic.BaseModel):
    name: str
    active: bool
    description: str | None


@router.post('')
async def create_base(
    base: BaseToCreate,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> Base:
    result = (
        connection
        .execute(
            "INSERT INTO bases(active, name, description) VALUES (%s, %s, %s) RETURNING *;",
            (base.active, base.name, base.description)
        )
        .fetchone()
    )
    return Base(
        id=result[0],
        active=result[1],
        name=result[2],
        description=result[3],
    )


@router.get('')
async def get_base_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> Base:
    base_row = (
        connection
        .execute("SELECT * FROM bases WHERE id = %s;", (id,))
        .fetchone()
    )

    if base_row is None:
        raise fastapi.HTTPException(404, f"Base not found: id={id}.")
    return Base(
        id=base_row[0],
        active=base_row[1],
        name=base_row[2],
        description=base_row[3],
    )


@router.delete('', status_code=fastapi.status.HTTP_204_NO_CONTENT)
async def delete_base_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> None:
    cursor = connection.cursor()
    try:
        result = (
            cursor
            .execute("DELETE FROM bases WHERE id = %s;", (id,))
        )
        if result.rowcount == 0:
            raise fastapi.HTTPException(404, f"Base not found: id={id}.")
    finally:
        cursor.close()
