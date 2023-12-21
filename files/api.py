import typing

import fastapi
import psycopg
import pydantic

from dependencies import get_connection


__all__ = ['router']


router = fastapi.APIRouter(
    prefix='/files',
    tags=['files'],
)


class File(pydantic.BaseModel):
    id: pydantic.UUID4
    category_id: pydantic.UUID4
    name: str = pydantic.Field(..., min_length=1)
    fingerprint: typing.Mapping[str, typing.Any] | None


class FileToCreate(pydantic.BaseModel):
    name: str
    category_id: pydantic.UUID4


@router.post('')
async def create_file(
    file: FileToCreate,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> File:
    try:
        result = (
            connection
            .execute(
                "INSERT INTO files(category_id, name) VALUES (%s, %s) RETURNING *;",
                (
                    file.category_id,
                    file.name,
                )
            )
            .fetchone()
        )
    except psycopg.errors.ForeignKeyViolation:
        raise fastapi.HTTPException(404, f"Category not found: id={file.category_id}.")
    return File(
        id=result[0],
        category_id=result[1],
        name=result[2],
        fingerprint=result[3],
    )


@router.get('')
async def get_file_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> File:
    file_row = (
        connection
        .execute(
            "SELECT * FROM files WHERE id = %s;",
            (id,)
        )
        .fetchone()
    )

    if file_row is None:
        raise fastapi.HTTPException(404, f"File not found: id={id}.")
    return File(
        id=file_row[0],
        category_id=file_row[1],
        name=file_row[2],
        fingerprint=file_row[3],
    )


@router.delete('', status_code=fastapi.status.HTTP_204_NO_CONTENT)
async def delete_file_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> None:
    cursor = connection.cursor()
    try:
        result = (
            cursor
            .execute("DELETE FROM files WHERE id = %s;", (id,))
        )
        if result.rowcount == 0:
            raise fastapi.HTTPException(404, f"File not found: id={id}.")
    finally:
        cursor.close()
