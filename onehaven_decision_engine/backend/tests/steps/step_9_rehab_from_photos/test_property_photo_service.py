from __future__ import annotations

from app.services.zillow_photo_source import classify_photo_kind


def test_classify_photo_kind_is_conservative():
    assert classify_photo_kind("https://photos.zillowstatic.com/fp/kitchen-shot.jpg") == "interior"
    assert classify_photo_kind("https://photos.zillowstatic.com/fp/front-exterior.jpg") == "exterior"
    assert classify_photo_kind("https://photos.zillowstatic.com/fp/random-a.jpg") == "unknown"