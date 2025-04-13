import json
import os
from typing import List

from src.scripts.data_warehouse.models.warehouse import (
    Metrics,
    Camps,
    Sites,
    SessionLocal,
)
from src.utils.logging import LOGGER

HERE = os.path.dirname(os.path.abspath(__file__))          
STATIC_DIR = os.path.join(HERE, "static")                
def _json_path(fname: str) -> str:
    """Return the absolute path of a file inside the static folder."""
    return os.path.join(STATIC_DIR, fname)


def _load_json(path: str):
    """Safely read a JSON file and return its data (or None on error)."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        LOGGER.error("Configuration file not found: %s", path)
    except json.JSONDecodeError as e:
        LOGGER.error("JSON decode error in %s: %s", path, e)
    except OSError as e:
        LOGGER.error("Could not read %s: %s", path, e)
    return None


def load_metrics_from_json(path: str = _json_path("metrics.json")) -> None:
    """Upsert metric rows from JSON."""
    data = _load_json(path)
    if data is None:
        return

    with SessionLocal() as session:
        try:
            for d in data:
                metric_id = d.get("id")
                if metric_id is None:
                    LOGGER.warning("Skipping metric without id: %s", d)
                    continue

                obj = session.get(Metrics, metric_id)
                if obj:
                    for k, v in d.items():
                        setattr(obj, k, v)
                    LOGGER.info("Updated metric id=%s (%s)", obj.id, obj.metric_name)
                else:
                    session.add(Metrics(**d))
                    LOGGER.info("Inserted new metric id=%s (%s)", metric_id, d.get("metric_name"))
            session.commit()
        except Exception as e:
            LOGGER.exception("load_metrics_from_json failed: %s", e)
            session.rollback()


def load_camps_from_json(path: str = _json_path("camps.json")) -> None:
    """Upsert camp rows from JSON."""
    data = _load_json(path)
    if data is None:
        return

    with SessionLocal() as session:
        try:
            for d in data:
                name = (d.get("CAMPNAME") or d.get("name") or "").strip()
                lat  = d.get("LAT")
                lon  = d.get("LONG") or d.get("LON")
                if not (name and lat and lon):
                    LOGGER.warning("Skipping camp with missing fields: %s", d)
                    continue

                obj = session.query(Camps).filter(
                    Camps.name.ilike(name)  
                ).one_or_none()

                if obj:
                    obj.lat  = float(lat)
                    obj.long = float(str(lon).strip())
                    LOGGER.info("Updated camp %s", name)
                else:
                    session.add(
                        Camps(name=name, lat=float(lat), long=float(str(lon).strip()))
                    )
                    LOGGER.info("Inserted new camp %s", name)
            session.commit()
        except Exception as e:
            LOGGER.exception("load_camps_from_json failed: %s", e)
            session.rollback()


def load_sites_from_json(path: str = _json_path("sites.json")) -> None:
    """Upsert site rows from JSON."""
    data = _load_json(path)
    if data is None:
        return

    with SessionLocal() as session:
        try:
            for d in data:
                site_id_raw = d.get("SITE_ID") or d.get("site_id")
                if site_id_raw is None:
                    LOGGER.warning("Skipping site without SITE_ID: %s", d)
                    continue
                try:
                    site_id = int(site_id_raw)
                except ValueError:
                    LOGGER.warning("Invalid SITE_ID %s – skipping", site_id_raw)
                    continue

                obj = session.get(Sites, site_id)
                if obj:
                    obj.site_name    = d.get("SITE_NAME")
                    obj.command_name = d.get("COMMAND_NAME")
                    obj.store_format = d.get("STORE_FORMAT")
                    LOGGER.info("Updated site_id %s", site_id)
                else:
                    session.add(
                        Sites(
                            site_id      = site_id,
                            site_name    = d.get("SITE_NAME"),
                            command_name = d.get("COMMAND_NAME"),
                            store_format = d.get("STORE_FORMAT"),
                        )
                    )
                    LOGGER.info("Inserted new site_id %s", site_id)
            session.commit()
        except Exception as e:
            LOGGER.exception("load_sites_from_json failed: %s", e)
            session.rollback()


if __name__ == "__main__":
    load_metrics_from_json() 
    load_camps_from_json()    
    load_sites_from_json()   
