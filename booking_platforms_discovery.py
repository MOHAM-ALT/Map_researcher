# web_discovery/booking_platforms_discovery.py
import requests
import json
import time
import random
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import re

class BookingPlatformsDiscovery:
    """Class for discovering hotels through booking platforms"""
    
    def __init__(self, db=None, config=None):
        """Initialize the BookingPlatformsDiscovery class"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("booking_platforms_discovery")
        self.driver = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8'
        }
        
    def initialize_webdriver(self):
        """Initialize the Selenium WebDriver"""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Add user agent to avoid detection
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        options.add_argument(f'user-agent={user_agent}')
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self.logger.info("WebDriver initialized successfully")
        
    def close_webdriver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.logger.info("WebDriver closed")
    
    def discover_hotels_in_area(self, location, checkin_date=None, checkout_date=None, platforms=None):
        """
        Discover hotels in the specified area using various booking platforms
        
        Args:
            location: Location name or coordinates
            checkin_date: Check-in date (optional, defaults to tomorrow)
            checkout_date: Check-out date (optional, defaults to day after tomorrow)
            platforms: List of platforms to search (optional, defaults to all)
            
        Returns:
            List of discovered hotel properties
        """
        self.logger.info(f"Starting discovery for location: {location}")
        
        # Set default dates if not provided
        if not checkin_date:
            checkin_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        if not checkout_date:
            checkout_date = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            
        # Set default platforms if not provided
        if not platforms:
            platforms = ["booking", "airbnb", "hotels", "agoda", "expedia", "local_platforms"]
            
        # Initialize WebDriver if needed
        if not self.driver:
            self.initialize_webdriver()
            
        all_results = []
        
        # Search each platform
        for platform in platforms:
            try:
                platform_method = getattr(self, f"_search_{platform}", None)
                if platform_method:
                    self.logger.info(f"Searching platform: {platform}")
                    results = platform_method(location, checkin_date, checkout_date)
                    
                    # Add platform name to results
                    for result in results:
                        result["platform"] = platform
                        
                    all_results.extend(results)
                    self.logger.info(f"Found {len(results)} results from {platform}")
                    
                    # Add random delay between platform searches
                    time.sleep(random.uniform(2, 5))
                else:
                    self.logger.warning(f"Search method not implemented for platform: {platform}")
            except Exception as e:
                self.logger.error(f"Error searching {platform}: {str(e)}")
                
        # Deduplicate results
        unique_results = self._remove_duplicates(all_results)
        self.logger.info(f"Total unique results: {len(unique_results)}")
        
        return unique_results
    
    def _search_booking(self, location, checkin_date, checkout_date):
        """
        Search for hotels on Booking.com
        
        Args:
            location: Location name
            checkin_date: Check-in date in YYYY-MM-DD format
            checkout_date: Check-out date in YYYY-MM-DD format
            
        Returns:
            List of hotel properties
        """
        try:
            # Format dates for Booking.com URL
            checkin_parts = checkin_date.split("-")
            checkout_parts = checkout_date.split("-")
            
            checkin_formatted = f"{checkin_parts[0]}-{checkin_parts[1]}-{checkin_parts[2]}"
            checkout_formatted = f"{checkout_parts[0]}-{checkout_parts[1]}-{checkout_parts[2]}"
            
            # Format the URL
            url = f"https://www.booking.com/searchresults.html?ss={location}&checkin={checkin_formatted}&checkout={checkout_formatted}&lang=ar"
            
            self.driver.get(url)
            
            # Wait for results to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='property-card']"))
            )
            
            # Scroll to load more results
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for _ in range(5):  # Scroll a few times
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Extract hotel data
            property_cards = self.driver.find_elements(By.CSS_SELECTOR, "div[data-testid='property-card']")
            
            results = []
            for card in property_cards:
                try:
                    # Extract hotel name
                    name_element = card.find_element(By.CSS_SELECTOR, "div[data-testid='title']")
                    name = name_element.text
                    
                    # Extract URL
                    link_element = card.find_element(By.CSS_SELECTOR, "a[data-testid='title-link']")
                    url = link_element.get_attribute("href")
                    
                    # Extract address if available
                    address = ""
                    address_elements = card.find_elements(By.CSS_SELECTOR, "span[data-testid='address']")
                    if address_elements:
                        address = address_elements[0].text
                    
                    # Extract rating if available
                    rating = ""
                    rating_elements = card.find_elements(By.CSS_SELECTOR, "div[data-testid='review-score']")
                    if rating_elements:
                        rating = rating_elements[0].text.split('\n')[0]
                    
                    # Extract price if available
                    price = ""
                    price_elements = card.find_elements(By.CSS_SELECTOR, "span[data-testid='price-and-discounted-price']")
                    if price_elements:
                        price = price_elements[0].text
                    
                    # Extract latitude and longitude from URL if available
                    lat, lng = None, None
                    if "city=" in url:
                        try:
                            # Try to extract coordinates from URL in cases where they're included
                            coords_part = url.split(";center=")[1].split(";")[0] if ";center=" in url else None
                            if coords_part:
                                coords = coords_part.split(",")
                                lat = float(coords[0])
                                lng = float(coords[1])
                        except:
                            pass
                    
                    hotel_data = {
                        "name": name,
                        "address": address,
                        "rating": rating,
                        "price": price,
                        "url": url,
                        "latitude": lat,
                        "longitude": lng,
                        "source": "booking.com",
                        "data_timestamp": datetime.now().isoformat()
                    }
                    
                    results.append(hotel_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting property data: {str(e)}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in _search_booking: {str(e)}")
            return []
    
    def _search_airbnb(self, location, checkin_date, checkout_date):
        """
        Search for properties on Airbnb
        
        Args:
            location: Location name
            checkin_date: Check-in date in YYYY-MM-DD format
            checkout_date: Check-out date in YYYY-MM-DD format
            
        Returns:
            List of properties
        """
        try:
            # Format dates for Airbnb URL
            checkin_formatted = checkin_date.replace("-", "")
            checkout_formatted = checkout_date.replace("-", "")
            
            # Format the URL
            url = f"https://www.airbnb.com/s/{location}/homes?checkin={checkin_formatted}&checkout={checkout_formatted}"
            
            self.driver.get(url)
            
            # Wait for results to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[itemprop='itemListElement']"))
            )
            
            # Scroll to load more results
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for _ in range(5):  # Scroll a few times
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Extract property data
            property_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[itemprop='itemListElement']")
            
            results = []
            for element in property_elements:
                try:
                    # Extract property name
                    name_element = element.find_element(By.CSS_SELECTOR, "meta[itemprop='name']")
                    name = name_element.get_attribute("content")
                    
                    # Extract URL
                    url_element = element.find_element(By.CSS_SELECTOR, "meta[itemprop='url']")
                    url = url_element.get_attribute("content")
                    
                    # Extract price if available
                    price = ""
                    price_elements = element.find_elements(By.CSS_SELECTOR, "span[data-testid='price-element']")
                    if price_elements:
                        price = price_elements[0].text
                    
                    # Extract rating if available
                    rating = ""
                    rating_elements = element.find_elements(By.CSS_SELECTOR, "span[aria-label*='rating']")
                    if rating_elements:
                        rating = rating_elements[0].get_attribute("aria-label").split()[0]
                    
                    # Extract type if available
                    property_type = ""
                    type_elements = element.find_elements(By.CSS_SELECTOR, "div[data-testid='listing-card-subtitle']")
                    if type_elements:
                        property_type = type_elements[0].text
                    
                    # We don't have direct access to coordinates, but can extract property ID
                    property_id = None
                    if url:
                        try:
                            property_id = url.split("/rooms/")[1].split("?")[0]
                        except:
                            pass
                    
                    property_data = {
                        "name": name,
                        "price": price,
                        "rating": rating,
                        "property_type": property_type,
                        "url": url,
                        "property_id": property_id,
                        "source": "airbnb",
                        "data_timestamp": datetime.now().isoformat()
                    }
                    
                    results.append(property_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting property data: {str(e)}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in _search_airbnb: {str(e)}")
            return []
    
    def _search_local_platforms(self, location, checkin_date, checkout_date):
        """
        Search for properties on local booking platforms
        
        Args:
            location: Location name
            checkin_date: Check-in date in YYYY-MM-DD format
            checkout_date: Check-out date in YYYY-MM-DD format
            
        Returns:
            List of properties
        """
        # This is a placeholder for local platforms
        # Implement searches for platforms like Almosafer, Flyin, etc.
        return []
        
    def _search_haraj(self, location):
        """
        Search for daily rental properties on Haraj
        
        Args:
            location: Location name
            
        Returns:
            List of properties
        """
        try:
            # Format the URL for Haraj search
            search_terms = [
                "شقق للإيجار اليومي",
                "شقق مفروشة يومي",
                "شقق فندقية",
                "إيجار يومي",
                "أجنحة فندقية"
            ]
            
            all_results = []
            
            for term in search_terms:
                url = f"https://haraj.com.sa/search/{term}%20{location}"
                
                self.driver.get(url)
                
                # Wait for results to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.post"))
                )
                
                # Extract property data
                post_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.post")
                
                for post in post_elements:
                    try:
                        # Extract title
                        title_element = post.find_element(By.CSS_SELECTOR, "a.postTitle")
                        title = title_element.text
                        
                        # Extract URL
                        url = title_element.get_attribute("href")
                        
                        # Extract city if available
                        city = ""
                        city_elements = post.find_elements(By.CSS_SELECTOR, "a.city")
                        if city_elements:
                            city = city_elements[0].text
                        
                        # Extract price if available
                        price = ""
                        price_elements = post.find_elements(By.CSS_SELECTOR, "div.postPrice")
                        if price_elements:
                            price = price_elements[0].text
                        
                        # Extract date if available
                        date = ""
                        date_elements = post.find_elements(By.CSS_SELECTOR, "span.postDate")
                        if date_elements:
                            date = date_elements[0].text
                        
                        property_data = {
                            "title": title,
                            "city": city,
                            "price": price,
                            "date_posted": date,
                            "url": url,
                            "source": "haraj",
                            "search_term": term,
                            "data_timestamp": datetime.now().isoformat()
                        }
                        
                        all_results.append(property_data)
                        
                    except Exception as e:
                        self.logger.warning(f"Error extracting property data: {str(e)}")
                        continue
                
                # Add delay between searches
                time.sleep(random.uniform(1, 3))
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"Error in _search_haraj: {str(e)}")
            return []
    
    def _remove_duplicates(self, properties):
        """
        Remove duplicate properties from the results
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            Deduplicated list of properties
        """
        unique_properties = []
        seen_urls = set()
        seen_names = {}
        
        for prop in properties:
            # If URL is available and not seen before, add property
            if prop.get("url") and prop["url"] not in seen_urls:
                seen_urls.add(prop["url"])
                unique_properties.append(prop)
                continue
            
            # If name is available, check for similarity with seen names
            if prop.get("name"):
                duplicate = False
                for seen_name, seen_index in seen_names.items():
                    # Check if names are very similar
                    similarity = self._calculate_string_similarity(prop["name"], seen_name)
                    if similarity > 0.8:
                        # It's likely a duplicate, check platform
                        if unique_properties[seen_index].get("platform") != prop.get("platform"):
                            # Different platforms, update the existing entry
                            unique_properties[seen_index]["other_platforms"] = unique_properties[seen_index].get("other_platforms", [])
                            unique_properties[seen_index]["other_platforms"].append({
                                "platform": prop.get("platform"),
                                "url": prop.get("url"),
                                "price": prop.get("price")
                            })
                        duplicate = True
                        break
                
                if not duplicate:
                    seen_names[prop["name"]] = len(unique_properties)
                    unique_properties.append(prop)
        
        return unique_properties
    
    def _calculate_string_similarity(self, str1, str2):
        """Calculate similarity between two strings using Levenshtein distance"""
        if not str1 or not str2:
            return 0
        
        # Convert to lowercase
        s1 = str1.lower()
        s2 = str2.lower()
        
        # Simple Levenshtein implementation
        rows = len(s1) + 1
        cols = len(s2) + 1
        dist = [[0 for x in range(cols)] for x in range(rows)]
        
        for i in range(rows):
            dist[i][0] = i
        
        for i in range(cols):
            dist[0][i] = i
        
        for i in range(1, rows):
            for j in range(1, cols):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                dist[i][j] = min(
                    dist[i-1][j] + 1,      # deletion
                    dist[i][j-1] + 1,      # insertion
                    dist[i-1][j-1] + cost  # substitution
                )
        
        max_len = max(len(s1), len(s2))
        similarity = 1 - (dist[rows-1][cols-1] / max_len) if max_len > 0 else 0
        return similarity

    def search_twitter_for_rentals(self, location, search_term=None):
        """
        Search Twitter for mentions of daily rentals in a specific location
        
        Args:
            location: Location name
            search_term: Additional search term (optional)
            
        Returns:
            List of tweets mentioning rentals
        """
        pass
        
    def search_property_forums(self, location):
        """
        Search property forums for daily rentals
        
        Args:
            location: Location name
            
        Returns:
            List of forum posts mentioning rentals
        """
        pass