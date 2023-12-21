import typing

import fastapi
import psycopg
import pydantic

from dependencies import get_connection


__all__ = ['router']


router = fastapi.APIRouter(
    prefix='/reports',
    tags=['reports'],
)


class ActiveFile(pydantic.BaseModel):
    name: str
    fingerprint: typing.Mapping[str, typing.Any]


class ActiveFiles(pydantic.BaseModel):
    files: list[ActiveFile]


@router.get('/active-files')
async def get_active_files(
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> ActiveFiles:
    result = (
        connection
        .execute("""
        SELECT f.name, f.fingerprint 
        FROM files AS f 
        JOIN categories AS c ON f.category_id = c.id 
        JOIN bases AS b ON b.id = c.base_id 
        WHERE c.active AND b.active
        """)
        .fetchall()
    )

    if not result:
        raise fastapi.HTTPException(404, f"Files not found.")
    return ActiveFiles(
        files=[ActiveFile(name=n, fingerprint=f) for n, f in result]
    )


class Base(pydantic.BaseModel):
    id: pydantic.UUID4
    active: bool
    name: str
    description: str | None


class Category(pydantic.BaseModel):
    id: pydantic.UUID4
    base_id: pydantic.UUID4
    active: bool
    sip_code: int
    sip_cause: str
    name: str
    description: str | None
    base: Base


class File(pydantic.BaseModel):
    name: str
    category: Category


@router.get('/jsonified-files')
async def get_jsonified_files(
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> list[File]:
    records = (
        connection
        .execute("""
        WITH categories_with_json_base AS (
            SELECT c.*, to_json(b.*) AS base
            FROM "categories" AS c
            JOIN "bases" AS b ON b."id" = c."base_id"
        )
        SELECT f."name", to_json(c.*) AS category
        FROM "files" AS f
        JOIN "categories_with_json_base" AS c ON c."id" = f."category_id"
        """)
        .fetchall()
    )

    if not records:
        raise fastapi.HTTPException(404, f"Files not found.")

    return [
        File(name=n, category=Category.model_validate(c))
        for n, c in records
    ]
