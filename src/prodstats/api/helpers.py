from typing import Dict, no_type_check

from fastapi import APIRouter


@no_type_check
def list_routes(router: APIRouter) -> Dict[str, Dict]:
    routes = {}
    for route in router.routes:
        routes[route.path] = {"name": route.name, "method": route.methods}

    return routes
