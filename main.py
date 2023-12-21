import fastapi
from dotenv import load_dotenv

from bases.api import router as bases_router
from categories.api import router as categories_router
from files.api import router as files_router
from reports.api import router as reports_router


load_dotenv()

app = fastapi.FastAPI()

app.include_router(bases_router)
app.include_router(categories_router)
app.include_router(files_router)
app.include_router(reports_router)
