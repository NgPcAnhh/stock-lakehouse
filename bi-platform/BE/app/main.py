from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import get_settings
from app.core.middleware import (
    CacheControlMiddleware,
    DBSemaphoreMiddleware,
    RateLimitMiddleware,
    TimeoutMiddleware,
)
from app.database.database import close_db, init_bi_db
from app.core.cache import close_redis
from app.modules.auth.router import router as auth_router
from app.modules.admin.router import router as admin_router

settings = get_settings()


# ── Lifecycle: startup / shutdown ──────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — các connection pool đã lazy-init, không cần gì thêm
    await init_bi_db()
    yield
    # Shutdown — đóng sạch connection pools
    await close_db()
    await close_redis()


from app.core.responses import SafeJSONResponse

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
    lifespan=lifespan,
    default_response_class=SafeJSONResponse,
)

# ── GZip middleware — nén response > 500 bytes (giảm ~70% cho JSON lớn) ──
app.add_middleware(GZipMiddleware, minimum_size=500)

# ── Cache-Control headers cho browser caching ──
app.add_middleware(CacheControlMiddleware)

# ── Rate limiting — 120 requests/phút/IP ──
app.add_middleware(RateLimitMiddleware, max_requests=120, window_seconds=60)

# ── Request timeout — 30 giây ──
app.add_middleware(TimeoutMiddleware, timeout_seconds=30.0)

# ── DB concurrency guard — max 50 concurrent DB requests ──
app.add_middleware(DBSemaphoreMiddleware, max_concurrent=50)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include BI routers
from app.modules.bi.data_sources.router import router as data_sources_router
from app.modules.bi.queries.router import router as queries_router
from app.modules.bi.datasets.router import router as datasets_router
from app.modules.bi.charts.router import router as charts_router
from app.modules.bi.dashboards.router import router as dashboards_router
from app.modules.bi.permissions.router import router as permissions_router

# Include routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")
app.include_router(data_sources_router, prefix="/api/v1/data-sources", tags=["data-sources"])
app.include_router(queries_router, prefix="/api/v1/queries", tags=["queries"])
app.include_router(datasets_router, prefix="/api/v1/datasets", tags=["datasets"])
app.include_router(charts_router, prefix="/api/v1/charts", tags=["charts"])
app.include_router(dashboards_router, prefix="/api/v1/dashboards", tags=["dashboards"])
app.include_router(permissions_router, prefix="/api/v1/permissions", tags=["chart-permissions"])

@app.get("/")
async def read_root():
    return {"message": "Welcome to BI Platform API"}