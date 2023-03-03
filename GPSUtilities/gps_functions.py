# Databricks notebook source
import numpy as np

def convert_steepbank_xy_to_utm(x: float, y: float, z: float = 100) -> dict:
    """
    Converts Steepbank (m) x, y coordinates to UTM coordinates. This formula was provided by Suncor's GIS Team.

    Args:
        x: x-coordinate of the point.
        y: y-coordinate of the point.

    Returns:
        dict: A dictionary containing the easting, northing, and elevation coordinates.

    """

    sf = 0.9995544496
    rotx = -0.0000521667
    roty = 0.0004096111
    rotz = 0.3327669444
    cx = 324468.4532
    cy = 6064857.9596
    cz = -1.1712

    easting = cx + sf * (
        x * np.cos(roty * (np.pi/180)) * np.cos(rotz*(np.pi/180)) +
        y * np.cos(roty*(np.pi/180)) * np.sin(rotz*(np.pi/180)) -
        z * np.sin(roty*(np.pi/180))
    )

    northing = cy + sf * (
        x * (
            np.sin(rotx * (np.pi/180)) * np.sin(roty*(np.pi/180)) * np.cos(rotz*(np.pi/180)) -
            np.cos(rotx*(np.pi/180)) * np.sin(rotz*(np.pi/180))
        ) + y * (
            np.sin(rotx*(np.pi/180)) * np.sin(roty*(np.pi/180)) * np.sin(rotz*(np.pi/180)) +
            np.cos(rotx*(np.pi/180)) * np.cos(rotz*(np.pi/180))
        ) + z * (
            np.sin(rotx*(np.pi/180)) * np.cos(roty*(np.pi/180))
        )
    )

    elevation = cz + sf * (
        x * (
            np.cos(rotx * (np.pi/180)) * np.sin(roty * (np.pi/180)) * np.cos(rotz * (np.pi/180)) +
            np.sin(rotx * (np.pi/180)) * np.sin(rotz * (np.pi/180))
        ) + y * (
            np.cos(rotx*(np.pi/180)) * np.sin(roty*(np.pi/180)) * np.sin(rotz*(np.pi/180)) -
            np.sin(rotx*(np.pi/180)) * np.cos(rotz*(np.pi/180))
        ) + z * (
            np.cos(rotx*(np.pi/180)) * np.cos(roty*(np.pi/180))
        )
    )
    
    coords = {'easting': easting
            , 'northing': northing
            , 'elevation': elevation
             }

    return coords
