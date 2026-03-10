from __future__ import annotations

import json

from app.routers import properties as properties_router


def test_extract_zillow_photo_urls_keeps_only_image_urls():
    raw = {
        "photos": [
            "https://photos.zillowstatic.com/fp/abc123-p_e.jpg",
            "https://example.com/not-image.txt",
            "https://photos.zillowstatic.com/fp/def456-p_f.webp",
        ],
        "nested": {
            "gallery": [
                {"url": "https://photos.zillowstatic.com/fp/ghi789-p_a.png"},
                {"url": "https://example.com/page"},
            ]
        },
    }

    urls = properties_router._extract_zillow_photo_urls(json.dumps(raw))

    assert len(urls) == 3
    assert urls[0].startswith("https://photos.zillowstatic.com/")
    assert urls[1].endswith(".webp")
    assert urls[2].endswith(".png")


def test_extract_zillow_photo_urls_dedupes_and_preserves_order():
    raw = {
        "photos": [
            "https://photos.zillowstatic.com/fp/dup-p_a.jpg",
            "https://photos.zillowstatic.com/fp/dup-p_a.jpg",
            "https://photos.zillowstatic.com/fp/unique-p_b.jpg",
        ]
    }

    urls = properties_router._extract_zillow_photo_urls(json.dumps(raw))

    assert urls == [
        "https://photos.zillowstatic.com/fp/dup-p_a.jpg",
        "https://photos.zillowstatic.com/fp/unique-p_b.jpg",
    ]