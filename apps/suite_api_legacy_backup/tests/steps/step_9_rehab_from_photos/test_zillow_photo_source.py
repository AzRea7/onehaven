from __future__ import annotations

from products.intelligence.backend.src.services.zillow_photo_source import extract_zillow_photo_urls


def test_extract_zillow_photo_urls_keeps_only_images():
    raw = {
        "photos": [
            "https://photos.zillowstatic.com/fp/a.jpg",
            "https://photos.zillowstatic.com/fp/b.webp",
            "https://example.com/not-image.txt",
        ],
        "nested": {
            "gallery": [
                {"url": "https://photos.zillowstatic.com/fp/c.png"},
                {"url": "https://example.com/page"},
            ]
        },
    }

    urls = extract_zillow_photo_urls(raw)
    assert urls == [
        "https://photos.zillowstatic.com/fp/a.jpg",
        "https://photos.zillowstatic.com/fp/b.webp",
        "https://photos.zillowstatic.com/fp/c.png",
    ]


def test_extract_zillow_photo_urls_dedupes_preserves_order():
    raw = {
        "photos": [
            "https://photos.zillowstatic.com/fp/dup.jpg",
            "https://photos.zillowstatic.com/fp/dup.jpg",
            "https://photos.zillowstatic.com/fp/unique.jpg",
        ]
    }

    urls = extract_zillow_photo_urls(raw)
    assert urls == [
        "https://photos.zillowstatic.com/fp/dup.jpg",
        "https://photos.zillowstatic.com/fp/unique.jpg",
    ]