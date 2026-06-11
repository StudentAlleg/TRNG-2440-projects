"""
Location data structure
"""
from unicodedata import name


class Location:
    """
    Represents a location
    """
    def __init__(self, name: str, latitude: float, longitude: float):
        self.name = name
        self.latitude = latitude
        self.longitude = longitude

    def get_name(self):
        return self.name

    def get_latitude(self):
        return self.latitude

    def get_longitude(self):
        return self.longitude

    @staticmethod
    def from_json():
