"""Compress and resize images for mobile (React Native Image / ImageViewer)."""

import io
from PIL import Image

# 1440px longest side covers 3x retina on all phones with room for zoom.
MAX_DIMENSION = 1440
WEBP_QUALITY = 75


def compress_image(image_data: bytes) -> bytes:
    """Resize, strip metadata, and compress an image to WebP.

    Returns WebP bytes suitable for React Native Image / image-zoom-viewer.
    """
    with Image.open(io.BytesIO(image_data)) as img:
        img = img.convert("RGB")

        # Resize if either dimension exceeds MAX_DIMENSION
        if max(img.size) > MAX_DIMENSION:
            img.thumbnail((MAX_DIMENSION, MAX_DIMENSION), Image.LANCZOS)

        # Save as WebP (strip all metadata by default)
        out = io.BytesIO()
        img.save(out, format="WEBP", quality=WEBP_QUALITY, method=6)
        return out.getvalue()
