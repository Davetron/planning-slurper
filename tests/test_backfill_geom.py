"""Tests for coordinate conversion in backfill_geom.py"""
import pytest


def test_itm_to_wgs84_known_point():
    """OSI reference: Spire of Dublin is at ITM 715830, 734697 → WGS84 53.3498, -6.2603"""
    from backfill_geom import itm_to_wgs84
    lat, lon = itm_to_wgs84(715830, 734697)
    assert abs(lat - 53.3498) < 0.001, f"Latitude {lat} not close to 53.3498"
    assert abs(lon - (-6.2603)) < 0.001, f"Longitude {lon} not close to -6.2603"


def test_itm_to_wgs84_dun_laoghaire():
    """DLR area: approximately ITM 722000, 728000 → ~53.289, ~6.175"""
    from backfill_geom import itm_to_wgs84
    lat, lon = itm_to_wgs84(722000, 728000)
    assert 53.2 < lat < 53.4
    assert -6.3 < lon < -6.0


def test_is_valid_dublin_grid():
    """Grid coordinates within Dublin bounding box are valid."""
    from backfill_geom import is_valid_dublin_grid
    assert is_valid_dublin_grid(715000, 734000) is True
    assert is_valid_dublin_grid(0, 0) is False
    assert is_valid_dublin_grid(7614064, 4745758) is False
    assert is_valid_dublin_grid(None, None) is False
