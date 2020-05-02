from typing import Dict

from fastapi import APIRouter

router = APIRouter()


@router.get("/", response_model=Dict)
def health():
    return {"status": "ok"}
