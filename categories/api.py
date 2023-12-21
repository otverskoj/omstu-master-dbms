import typing

import fastapi
import psycopg
import pydantic

from dependencies import get_connection


__all__ = ['router']


router = fastapi.APIRouter(
    prefix='/categories',
    tags=['categories'],
)


class Category(pydantic.BaseModel):
    id: pydantic.UUID4
    active: bool
    sip_code: int = pydantic.Field(..., ge=400, le=699)
    sip_cause: str = pydantic.Field(..., min_length=1)
    name: str = pydantic.Field(..., min_length=1)
    description: str | None = None
    base_id: pydantic.UUID4


class CategoryToCreate(pydantic.BaseModel):
    active: bool
    sip_code: int = pydantic.Field(..., ge=400, le=699)
    sip_cause: str = pydantic.Field(..., min_length=1)
    name: str = pydantic.Field(..., min_length=1)
    description: str | None = None
    base_id: pydantic.UUID4


@router.post('')
async def create_category(
    category: CategoryToCreate,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> Category:
    try:
        result = (
            connection
            .execute(
                "INSERT INTO categories(base_id, active, sip_code, sip_cause, name, description) VALUES (%s, %s, %s, %s, %s, %s) RETURNING *;",
                (
                    category.base_id,
                    category.active,
                    category.sip_code,
                    category.sip_cause,
                    category.name,
                    category.description
                )
            )
            .fetchone()
        )
    except psycopg.errors.ForeignKeyViolation:
        raise fastapi.HTTPException(404, f"Base not found: id={category.base_id}.")
    return Category(
        id=result[0],
        base_id=result[1],
        active=result[2],
        sip_code=result[3],
        sip_cause=result[4],
        name=result[5],
        description=result[6],
    )


@router.get('')
async def get_category_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> Category:
    category_row = (
        connection
        .execute(
            "SELECT * FROM categories WHERE id = %s;",
            (id,)
        )
        .fetchone()
    )

    if category_row is None:
        raise fastapi.HTTPException(404, f"Category not found: id={id}.")
    return Category(
        id=category_row[0],
        base_id=category_row[1],
        active=category_row[2],
        sip_code=category_row[3],
        sip_cause=category_row[4],
        name=category_row[5],
        description=category_row[6],
    )


@router.delete('', status_code=fastapi.status.HTTP_204_NO_CONTENT)
async def delete_category_by_id(
    id: pydantic.UUID4,
    connection: typing.Annotated[psycopg.Connection, fastapi.Depends(get_connection)]
) -> None:
    cursor = connection.cursor()
    try:
        result = (
            cursor
            .execute("DELETE FROM categories WHERE id = %s;", (id,))
        )
        if result.rowcount == 0:
            raise fastapi.HTTPException(404, f"Category not found: id={id}.")
    finally:
        cursor.close()
