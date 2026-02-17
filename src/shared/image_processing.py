import io
from PIL import Image

# Processing defaults
MAX_WIDTH = 1200
WEBP_QUALITY = 65


def process_image_bytes(
    image_data: bytes,
    max_width: int = MAX_WIDTH,
    quality: int = WEBP_QUALITY,
) -> bytes:
    """Process an image: strip metadata, resize, compress, convert to WebP.

    Args:
        image_data: Raw image bytes.
        max_width: Maximum width in pixels (aspect ratio preserved).
        quality: WebP compression quality (0-100).

    Returns:
        Processed image as WebP bytes.
    """
    with Image.open(io.BytesIO(image_data)) as img:
        # Determine target mode (preserve transparency when present)
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            target_mode = "RGBA"
        else:
            target_mode = "RGB"

        img = img.convert(target_mode)

        # Strip EXIF / metadata by copying pixel data to a fresh image
        clean_img = Image.new(target_mode, img.size)
        clean_img.putdata(list(img.getdata()))

        # Resize if wider than max_width, keeping aspect ratio
        if clean_img.width > max_width:
            ratio = max_width / clean_img.width
            new_height = int(clean_img.height * ratio)
            clean_img = clean_img.resize((max_width, new_height), Image.LANCZOS)

        # Encode to WebP
        output = io.BytesIO()
        clean_img.save(output, format="WEBP", quality=quality, method=4)
        return output.getvalue()
