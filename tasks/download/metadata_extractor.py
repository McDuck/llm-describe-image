"""Extract metadata from images including EXIF data and filename/directory parsing.

Requires Pillow (PIL) for EXIF extraction: pip install Pillow
If Pillow is not available, datetime will be extracted from filename/path only.
"""

import os
import re
from typing import Optional, Tuple
from datetime import datetime

try:
    from PIL import Image  # type: ignore
    from PIL.ExifTags import TAGS, GPSTAGS  # type: ignore
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False


def extract_datetime_from_exif(image_path: str) -> Optional[datetime]:
    """Extract datetime from EXIF data."""
    if not PILLOW_AVAILABLE:
        return None
    
    try:
        with Image.open(image_path) as img:
            exif_data = img._getexif()
            if not exif_data:
                return None
            
            # Try different datetime tags
            for tag_id in [36867, 36868, 306]:  # DateTimeOriginal, DateTimeDigitized, DateTime
                if tag_id in exif_data:
                    datetime_str = exif_data[tag_id]
                    # Format: "YYYY:MM:DD HH:MM:SS"
                    try:
                        return datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S")
                    except (ValueError, TypeError):
                        continue
    except Exception:
        pass
    
    return None


def extract_datetime_from_filename(filename: str) -> Optional[datetime]:
    """Extract datetime from filename patterns like 20191114_122944 or 2019-11-14."""
    # Pattern: YYYYMMDD_HHMMSS (e.g., 20191114_122944)
    pattern1 = r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})'
    match = re.search(pattern1, filename)
    if match:
        try:
            year, month, day, hour, minute, second = map(int, match.groups())
            return datetime(year, month, day, hour, minute, second)
        except (ValueError, TypeError):
            pass
    
    # Pattern: YYYY-MM-DD (e.g., 2019-11-14)
    pattern2 = r'(\d{4})-(\d{2})-(\d{2})'
    match = re.search(pattern2, filename)
    if match:
        try:
            year, month, day = map(int, match.groups())
            return datetime(year, month, day)
        except (ValueError, TypeError):
            pass
    
    # Pattern: YYYYMMDD (e.g., 20191114)
    pattern3 = r'(\d{4})(\d{2})(\d{2})'
    match = re.search(pattern3, filename)
    if match:
        try:
            year, month, day = map(int, match.groups())
            return datetime(year, month, day)
        except (ValueError, TypeError):
            pass
    
    return None


def extract_datetime_from_path(file_path: str) -> Optional[datetime]:
    """Extract datetime from directory names in path."""
    # Check all directory names in the path
    parts = file_path.split(os.sep)
    
    for part in reversed(parts):  # Start from deepest directory
        dt = extract_datetime_from_filename(part)
        if dt:
            return dt
    
    return None


def extract_gps_location(image_path: str) -> Optional[Tuple[float, float]]:
    """Extract GPS coordinates from EXIF data. Returns (latitude, longitude)."""
    if not PILLOW_AVAILABLE:
        return None
    
    try:
        with Image.open(image_path) as img:
            exif_data = img._getexif()
            if not exif_data:
                return None
            
            # Get GPS info
            gps_info = {}
            for tag_id, value in exif_data.items():
                tag_name = TAGS.get(tag_id, tag_id)
                if tag_name == "GPSInfo":
                    for gps_tag_id in value:
                        gps_tag_name = GPSTAGS.get(gps_tag_id, gps_tag_id)
                        gps_info[gps_tag_name] = value[gps_tag_id]
                    break
            
            if not gps_info:
                return None
            
            # Convert GPS coordinates to decimal degrees
            def convert_to_degrees(value):
                d, m, s = value
                return float(d) + float(m) / 60.0 + float(s) / 3600.0
            
            lat = gps_info.get('GPSLatitude')
            lat_ref = gps_info.get('GPSLatitudeRef')
            lon = gps_info.get('GPSLongitude')
            lon_ref = gps_info.get('GPSLongitudeRef')
            
            if lat and lon and lat_ref and lon_ref:
                latitude = convert_to_degrees(lat)
                if lat_ref == 'S':
                    latitude = -latitude
                
                longitude = convert_to_degrees(lon)
                if lon_ref == 'W':
                    longitude = -longitude
                
                return (latitude, longitude)
    except Exception:
        pass
    
    return None


def get_image_metadata(image_path: str) -> dict:
    """
    Extract all relevant metadata from image.
    
    Returns dict with:
    - datetime: datetime object or None
    - datetime_source: 'exif', 'filename', 'path', or None
    - location: (latitude, longitude) tuple or None
    - location_str: formatted location string or None
    - camera: camera make/model or None
    - focal_length: focal length string or None
    - aperture: aperture (f-stop) string or None
    - iso: ISO value string or None
    - shutter_speed: shutter speed string or None
    - filename: base filename without extension
    """
    metadata = {
        'datetime': None,
        'datetime_source': None,
        'location': None,
        'location_str': None,
        'camera': None,
        'focal_length': None,
        'aperture': None,
        'iso': None,
        'shutter_speed': None,
        'filename': os.path.splitext(os.path.basename(image_path))[0]
    }
    
    # Extract camera and settings from EXIF
    if PILLOW_AVAILABLE:
        try:
            with Image.open(image_path) as img:
                exif_data = img._getexif()
                if exif_data:
                    # Camera make/model (tags 271, 272)
                    make = exif_data.get(271, '').strip()
                    model = exif_data.get(272, '').strip()
                    if make and model:
                        metadata['camera'] = f"{make} {model}"
                    elif model:
                        metadata['camera'] = model
                    elif make:
                        metadata['camera'] = make
                    
                    # Focal length (tag 37386)
                    if 37386 in exif_data:
                        focal = exif_data[37386]
                        if isinstance(focal, tuple):
                            metadata['focal_length'] = f"{focal[0]/focal[1]:.1f}mm"
                        else:
                            metadata['focal_length'] = f"{focal}mm"
                    
                    # Aperture/F-number (tag 33437)
                    if 33437 in exif_data:
                        aperture = exif_data[33437]
                        if isinstance(aperture, tuple):
                            metadata['aperture'] = f"f/{aperture[0]/aperture[1]:.1f}"
                        else:
                            metadata['aperture'] = f"f/{aperture:.1f}"
                    
                    # ISO (tag 34855)
                    if 34855 in exif_data:
                        metadata['iso'] = f"ISO {exif_data[34855]}"
                    
                    # Shutter speed (tag 33434)
                    if 33434 in exif_data:
                        shutter = exif_data[33434]
                        if isinstance(shutter, tuple):
                            if shutter[0] < shutter[1]:
                                metadata['shutter_speed'] = f"1/{shutter[1]//shutter[0]}s"
                            else:
                                metadata['shutter_speed'] = f"{shutter[0]/shutter[1]:.1f}s"
                        else:
                            metadata['shutter_speed'] = f"{shutter}s"
        except Exception:
            pass
    
    # Try EXIF datetime first
    dt = extract_datetime_from_exif(image_path)
    if dt:
        metadata['datetime'] = dt
        metadata['datetime_source'] = 'exif'
    else:
        # Try filename
        filename = os.path.basename(image_path)
        dt = extract_datetime_from_filename(filename)
        if dt:
            metadata['datetime'] = dt
            metadata['datetime_source'] = 'filename'
        else:
            # Try path
            dt = extract_datetime_from_path(image_path)
            if dt:
                metadata['datetime'] = dt
                metadata['datetime_source'] = 'path'
    
    # Extract GPS location
    location = extract_gps_location(image_path)
    if location:
        metadata['location'] = location
        lat, lon = location
        metadata['location_str'] = f"{lat:.6f}, {lon:.6f}"
    
    return metadata
