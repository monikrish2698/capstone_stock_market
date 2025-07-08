import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError
import json
import time
from typing import Dict, Any

def make_api_request(
        url: str,
        params: Dict[str, Any] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 10
):
    """
    Make an API request with comprehensive error handling.
    """

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()

            try:
                data = response.json()
                return True, data, None
            except json.JSONDecodeError as e:
                return False, None, f"Failed to parse JSON response: {e}"
        except Timeout:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False, None, f"Request timed out for URL: {url} (attempt {attempt + 1} / {max_retries})"
        
        except ConnectionError:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False, None, f"Connection error for URL: {url} (attempt {attempt + 1} / {max_retries})"
        
        except HTTPError as e:
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', 60)
                time.sleep(retry_after)
                continue
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False, None, f"Server error for URL: {url} (attempt {attempt + 1} / {max_retries})"
            else:
                return False, None, f"Client error {response.status_code}: {e}"
        
        except RequestException as e:
            error_msg = f"General request error: {e}"
            print(error_msg)
            return False, None, error_msg
            
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            print(error_msg)
            return False, None, error_msg
        
    return False, None, f"Request failed after {max_retries} attempts"