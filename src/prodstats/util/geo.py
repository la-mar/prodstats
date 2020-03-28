from pathlib import Path
from typing import Union

import geoalchemy2
import geopandas as gp
from geoalchemy2 import WKBElement
from shapely.geometry.base import BaseGeometry


def to_geojson(gdf: gp.GeoDataFrame, path: Union[str, Path]):
    if not isinstance(gdf, gp.GeoDataFrame):
        raise TypeError(f"'gdf' must be of type GeoDataFrame, not {type(gdf)}")
    path = Path(path)
    path.unlink(missing_ok=True)  # type: ignore
    return gdf.to_file(path, driver="GeoJSON")


def shape_to_wkb(
    shape: Union[BaseGeometry, WKBElement], srid: int = 4326
) -> WKBElement:
    if isinstance(shape, BaseGeometry):
        shape = geoalchemy2.shape.from_shape(shape, srid=srid)
    return shape


def wkb_to_shape(wkb: Union[WKBElement, BaseGeometry]) -> BaseGeometry:
    if isinstance(wkb, WKBElement):
        wkb = geoalchemy2.shape.to_shape(wkb)
    return wkb
