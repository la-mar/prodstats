from fastapi import APIRouter

from api.v1.endpoints.health import router as health_router
from api.v1.endpoints.tasks import router as task_router

__all__ = ["api_router"]

api_router = APIRouter()
api_router.include_router(health_router, prefix="/health", tags=["health"])
api_router.include_router(task_router, prefix="/tasks", tags=["tasks"])
