import os
import sys
import time
from typing import Optional, Tuple, Union
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class GeolocationTask(Task[str, Tuple[str, Optional[str]]]):
    """
    Reverse geocode GPS coordinates to human-readable location strings.
    
    Input: image path
    Output: (image_path, location_string)
    
    Skips images without GPS coordinates.
    Handles network errors with exponential backoff.
    """
    
    def __init__(
        self,
        maximum: int = 1,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        initial_wait_seconds: int = 1,
        max_retries: int = 5
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
        self.initial_wait_seconds: int = initial_wait_seconds
        self.max_retries: int = max_retries
    
    def execute(self, image_path: str) -> Tuple[str, Optional[str]]:
        """
        Reverse geocode GPS coordinates to human-readable location.
        
        Args: image_path
        Returns: (image_path, location_string or None)
        """
        try:
            from tasks.download.metadata_extractor import extract_gps_location
            
            # Extract GPS coordinates from image
            gps_location = extract_gps_location(image_path)
            if not gps_location:
                # No GPS data - return None (will be skipped or cached)
                return (image_path, None)
            
            latitude, longitude = gps_location
            
            # Reverse geocode with exponential backoff for network errors
            location_str = self._reverse_geocode_with_retry(latitude, longitude, image_path)
            return (image_path, location_str)
            
        except Exception as e:
            # Show relative path in error
            rel_path = image_path
            if self.input_dir and image_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(image_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Geolocation failed for {rel_path}: {str(e)}")
    
    def _reverse_geocode_with_retry(self, latitude: float, longitude: float, image_path: str) -> Optional[str]:
        """
        Reverse geocode with adaptive backoff for network errors (timeout, DDOS).
        On success: reduce wait time by half. On failure: double it.
        """
        from geopy.geocoders import Nominatim
        from geopy.exc import GeopyError, GeocoderTimedOut, GeocoderUnavailable
        
        wait_time = self.initial_wait_seconds
        rel_path = image_path
        if self.input_dir and image_path.startswith(self.input_dir):
            try:
                rel_path = os.path.relpath(image_path, self.input_dir)
            except (ValueError, TypeError):
                pass
        
        for attempt in range(self.max_retries):
            try:
                geolocator = Nominatim(user_agent="geolocation_task")
                location = geolocator.reverse(
                    f"{latitude}, {longitude}",
                    language='en',
                    zoom=18,
                    addressdetails=True
                )
                
                if location:
                    address = location.address
                    
                    # Try to extract landmark/business info from the address
                    raw = location.raw if hasattr(location, 'raw') else {}
                    address_parts = raw.get('address', {}) if isinstance(raw, dict) else {}
                    
                    # Extract all address components as POI info
                    poi_info = []
                    excluded_keys = {'country_code', 'country', 'state', 'county', 'city', 'town', 'village', 'postcode', 'road', 'house_number'}
                    for key, value in address_parts.items():
                        if key not in excluded_keys and value:
                            poi_info.append(f"{key}: {value}")
                    
                    if poi_info:
                        full_info = address + " | " + ", ".join(poi_info)
                        # Success - reduce wait time for next attempt by this task
                        wait_time = max(self.initial_wait_seconds, wait_time / 2)
                        return full_info
                    
                    # Success - reduce wait time
                    wait_time = max(self.initial_wait_seconds, wait_time / 2)
                    return address
                
                # Success - reduce wait time
                wait_time = max(self.initial_wait_seconds, wait_time / 2)
                return None
                
            except (GeocoderTimedOut, GeocoderUnavailable, TimeoutError, ConnectionError) as e:
                # Network error - increase backoff for next retry
                if attempt < self.max_retries - 1:
                    wait_time *= 2  # Double wait time on failure
                    print(f"Network error for {rel_path} (attempt {attempt + 1}/{self.max_retries}): {type(e).__name__}. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    # Final attempt failed
                    raise Exception(f"Geolocation timeout after {self.max_retries} attempts for {rel_path}: {str(e)}")
            
            except GeopyError as e:
                # Other geopy errors - don't retry
                raise Exception(f"Geolocation error for {rel_path}: {str(e)}")


class GeolocationWriteTask(Task[Tuple[str, Optional[str]], str]):
    """
    Write geolocation data to files.
    
    Input: (image_path, location_string)
    Output: image_path
    
    Success: writes to .geocode.txt
    Failure: writes to .geocode.error.txt
    """
    
    def __init__(
        self,
        maximum: int = 1,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
    
    def execute(self, item: Tuple[str, Union[str, Exception]]) -> str:
        """
        Write geolocation to file.
        Args: (input_path, location_string_or_error)
        Returns: output_path
        """
        # Handle both success (str) and error (Exception) cases
        if len(item) == 2:
            input_path, content_or_error = item
        else:
            input_path = item
            content_or_error = None
        
        # Calculate output paths
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(input_path, self.input_dir)
            output_file = os.path.join(self.output_dir, relative + ".geocode.txt")
            error_file = os.path.join(self.output_dir, relative + ".geocode.error.txt")
        else:
            output_file = input_path + ".geocode.txt"
            error_file = input_path + ".geocode.error.txt"
        
        # Check if this is an error or success
        if isinstance(content_or_error, Exception):
            # Write error file
            try:
                os.makedirs(os.path.dirname(error_file), exist_ok=True)
                with open(error_file, "w", encoding="utf-8") as f:
                    f.write(f"{str(content_or_error)}\n")
                return error_file
            except Exception as e:
                # Show relative path in error
                rel_path = input_path
                if self.input_dir and input_path.startswith(self.input_dir):
                    try:
                        rel_path = os.path.relpath(input_path, self.input_dir)
                    except (ValueError, TypeError):
                        pass
                try:
                    print(f"Error writing geolocation error file for {rel_path}: {e}")
                except:
                    pass
                raise
        else:
            # Write success output
            if content_or_error is None:
                # No GPS data - write "N/A" to indicate no location available
                content = "N/A"
            else:
                content = content_or_error
            
            try:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(content)
                return output_file
            except Exception as e:
                # Show relative path in error
                rel_path = input_path
                if self.input_dir and input_path.startswith(self.input_dir):
                    try:
                        rel_path = os.path.relpath(input_path, self.input_dir)
                    except (ValueError, TypeError):
                        pass
                try:
                    print(f"Error writing geolocation for {rel_path}: {e}")
                except:
                    pass
                raise

