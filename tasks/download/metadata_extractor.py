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

try:
    from geopy.geocoders import Nominatim  # type: ignore
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False


def extract_datetime_from_path_or_filename(file_path: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Extract datetime range from file path or filename patterns.
    Returns (min_datetime, max_datetime) tuple representing possible time range based on precision.
    
    Examples:
    - Full timestamp "20191114_122944" -> exact time: (2019-11-14 12:29:44, 2019-11-14 12:29:44)
    - Date only "2019-11-14" -> full day range: (2019-11-14 00:00:00, 2019-11-14 23:59:59)
    - Directory path "2022/2022-01/2022-01-03/" -> full day: (2022-01-03 00:00:00, 2022-01-03 23:59:59)
    """
    from datetime import timedelta
    
    # Try filename first
    filename = os.path.basename(file_path)
    
    # Pattern: YYYYMMDD_HHMMSS or YYYYMMDDHHMMSS (e.g., 20191114_122944 or 20191114122944)
    # Exact timestamp - return same min/max
    pattern1 = r'(\d{4})(\d{2})(\d{2})[_\-]?([0-2][0-9])([0-5][0-9])([0-5][0-9])'
    match = re.search(pattern1, filename)
    if match:
        try:
            year, month, day, hour, minute, second = map(int, match.groups())
            if 1 <= month <= 12 and 1 <= day <= 31:
                dt = datetime(year, month, day, hour, minute, second)
                return (dt, dt)  # Exact time known
        except (ValueError, TypeError):
            pass
    
    # Pattern: YYYY-MM-DD or YYYY_MM_DD in filename (e.g., 2019-11-14)
    # Date only - return full day range
    pattern2 = r'[\-_/]([1-2][0-9]{3})([0-1][0-9])([0-3][0-9])[\-_/]'
    match = re.search(pattern2, filename)
    if match:
        try:
            year, month, day = map(int, match.groups())
            if 1 <= month <= 12 and 1 <= day <= 31:
                min_dt = datetime(year, month, day, 0, 0, 0)
                max_dt = datetime(year, month, day, 23, 59, 59)
                return (min_dt, max_dt)
        except (ValueError, TypeError):
            pass
    
    # Pattern: YYYYMMDD at start of filename (e.g., 20191114)
    # Date only - return full day range
    pattern3 = r'^([1-2][0-9]{3})([0-1][0-9])([0-3][0-9])'
    match = re.search(pattern3, filename)
    if match:
        try:
            year, month, day = map(int, match.groups())
            if 1 <= month <= 12 and 1 <= day <= 31:
                min_dt = datetime(year, month, day, 0, 0, 0)
                max_dt = datetime(year, month, day, 23, 59, 59)
                return (min_dt, max_dt)
        except (ValueError, TypeError):
            pass
    
    # Fallback to directory names in path (deepest first)
    # Date only from directories - return full day range
    parts = file_path.split(os.sep)
    for part in reversed(parts[:-1]):  # Skip filename, check directories
        # Try YYYYMMDD pattern in directory
        match = re.search(pattern3, part)
        if match:
            try:
                year, month, day = map(int, match.groups())
                if 1 <= month <= 12 and 1 <= day <= 31:
                    min_dt = datetime(year, month, day, 0, 0, 0)
                    max_dt = datetime(year, month, day, 23, 59, 59)
                    return (min_dt, max_dt)
            except (ValueError, TypeError):
                pass
        
        # Try YYYY-MM-DD pattern in directory
        match = re.search(r'^([1-2][0-9]{3})-([0-1][0-9])-([0-3][0-9])', part)
        if match:
            try:
                year, month, day = map(int, match.groups())
                if 1 <= month <= 12 and 1 <= day <= 31:
                    min_dt = datetime(year, month, day, 0, 0, 0)
                    max_dt = datetime(year, month, day, 23, 59, 59)
                    return (min_dt, max_dt)
            except (ValueError, TypeError):
                pass
    
    return None


def extract_datetime_from_path(file_path: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Deprecated: use extract_datetime_from_path_or_filename instead.
    This function is kept for backwards compatibility.
    Returns (min_datetime, max_datetime) tuple.
    """
    return extract_datetime_from_path_or_filename(file_path)


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


def reverse_geocode_location(latitude: float, longitude: float) -> Optional[str]:
    """
    Reverse geocode GPS coordinates to human-readable location name.
    Returns location name (address, city, landmark, etc.) or None if unavailable.
    Requires geopy package: pip install geopy
    """
    if not GEOPY_AVAILABLE:
        return None
    
    try:
        geolocator = Nominatim(user_agent="llm-describe-image")
        location = geolocator.reverse(f"{latitude}, {longitude}", language='en')
        if location:
            return location.address
    except Exception:
        pass
    
    return None



def get_image_metadata(image_path: str) -> dict:
    """
    Extract all relevant metadata from image with single file open.
    
    Returns dict with:
    - datetime: datetime object (using min of range) or None
    - datetime_min: earliest possible datetime from filename/path or None
    - datetime_max: latest possible datetime from filename/path or None
    - datetime_source: 'exif', 'filename_or_path', or None
    - location: (latitude, longitude) tuple or None
    - location_str: formatted location string or None
    - location_address: reverse-geocoded address or None
    - camera: camera make/model or None
    - focal_length: focal length string or None
    - aperture: aperture (f-stop) string or None
    - iso: ISO value string or None
    - shutter_speed: shutter speed string or None
    - filename: full filename with extension
    """
    metadata = {
        'datetime': None,
        'datetime_min': None,
        'datetime_max': None,
        'datetime_source': None,
        'location': None,
        'location_str': None,
        'location_address': None,
        'camera': None,
        'focal_length': None,
        'aperture': None,
        'iso': None,
        'shutter_speed': None,
        'filename': os.path.basename(image_path)
    }
    
    # Extract all EXIF data in a single file open
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
                        try:
                            # Convert IFDRational to float first
                            focal_value = float(focal) if hasattr(focal, '__float__') else (focal[0]/focal[1] if isinstance(focal, tuple) else focal)
                            metadata['focal_length'] = f"{focal_value:.1f}mm"
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass
                    
                    # Aperture/F-number (tag 33437)
                    if 33437 in exif_data:
                        aperture = exif_data[33437]
                        try:
                            # Convert IFDRational to float first
                            aperture_value = float(aperture) if hasattr(aperture, '__float__') else (aperture[0]/aperture[1] if isinstance(aperture, tuple) else aperture)
                            metadata['aperture'] = f"f/{aperture_value:.1f}"
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass
                    
                    # ISO (tag 34855)
                    if 34855 in exif_data:
                        try:
                            iso_value = exif_data[34855]
                            metadata['iso'] = f"ISO {int(iso_value)}"
                        except (TypeError, ValueError):
                            pass
                    
                    # Shutter speed (tag 33434)
                    if 33434 in exif_data:
                        shutter = exif_data[33434]
                        try:
                            # Convert IFDRational to float first
                            if hasattr(shutter, '__float__'):
                                shutter_value = float(shutter)
                                if shutter_value < 1:
                                    metadata['shutter_speed'] = f"1/{int(1/shutter_value)}s"
                                else:
                                    metadata['shutter_speed'] = f"{shutter_value:.1f}s"
                            elif isinstance(shutter, tuple):
                                shutter_value = shutter[0] / shutter[1]
                                if shutter_value < 1:
                                    metadata['shutter_speed'] = f"1/{int(1/shutter_value)}s"
                                else:
                                    metadata['shutter_speed'] = f"{shutter_value:.1f}s"
                            else:
                                metadata['shutter_speed'] = f"{shutter}s"
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass
                    
                    # Extract datetime from EXIF (tags 36867, 36868, 306)
                    for tag_id in [36867, 36868, 306]:  # DateTimeOriginal, DateTimeDigitized, DateTime
                        if tag_id in exif_data:
                            datetime_str = exif_data[tag_id]
                            try:
                                dt = datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S")
                                metadata['datetime'] = dt
                                metadata['datetime_min'] = dt  # EXIF is exact - same min/max
                                metadata['datetime_max'] = dt
                                metadata['datetime_source'] = 'exif'
                                break
                            except (ValueError, TypeError):
                                continue
                    
                    # Extract GPS location
                    gps_location = extract_gps_location(image_path)
                    if gps_location:
                        metadata['location'] = gps_location
                        lat, lon = gps_location
                        metadata['location_str'] = f"{lat:.6f}, {lon:.6f}"
                        # Try to reverse geocode to human-readable address
                        address = reverse_geocode_location(lat, lon)
                        if address:
                            metadata['location_address'] = address
        except Exception as e:
            pass
    
    # Fallback to path/filename for datetime if not found in EXIF
    if not metadata['datetime']:
        dt_range = extract_datetime_from_path_or_filename(image_path)
        if dt_range:
            # Store both min and max for temporal uncertainty awareness
            min_dt, max_dt = dt_range
            metadata['datetime'] = min_dt  # Use min as primary (earliest possible)
            metadata['datetime_min'] = min_dt
            metadata['datetime_max'] = max_dt
            metadata['datetime_source'] = 'filename_or_path'
    
    return metadata
