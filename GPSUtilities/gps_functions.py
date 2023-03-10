# Databricks notebook source
import numpy as np
from typing import Dict

def convert_steepbank_xy_to_utm(x: float, y: float, z: float = 100) -> Dict[str, float]:
    """
    Converts Steepbank (m) x, y coordinates to UTM coordinates. This formula was provided by Suncor's GIS Team.

    Args:
        x: x-coordinate in Steepbank system.
        y: y-coordinate in Steepbank system.

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

def convert_steepbank_xy_to_lon_lat(x: float, y: float) -> Dict[str, float]:
    """
    Converts Steepbank (m) x, y coordinates to longitude and latitude. This formula was provided by Suncor's GIS Team.
    
    Args:
        x: x-coordinate in Steepbank system.
        y: y-coordinate in Steepbank system.
        
    Returns:
        dict: A dictionary containing the converted coordinates.
    """
    Scale_in = 0.004321953
    Scale_out = 0.003637212
    X1 = 0.137462175
    Xshift_in = -150169.25
    Xshift_out = -111.394916
    Xx = 1.044954407
    Xx2 = 6.90E-08
    Xxy = 5.81E-05
    Xy = 4.05E-05
    Xy2 = -6.83E-08
    Y1 = 0.035543067
    Yshift_in = -246627.855
    Yshift_out = 56.93738889
    Yx = -2.21E-05
    Yx2 = -1.59E-05
    Yxy = 7.63E-08
    Yy = 0.571219926
    Yy2 = -9.54E-08

    x = (x + Xshift_in) * Scale_in
    y = (y + Yshift_in) * Scale_in

    x_p = Xx * x + Xy * y + X1 + Xxy * x * y + Xx2 * x**2 + Xy2 * y**2
    y_p = Yx * x + Yy * y + Y1 + Yxy * x * y + Yx2 * x**2 + Yy2 * y**2

    coords = {'longitude': x_p * Scale_out + Xshift_out
            , 'latitude': y_p * Scale_out + Yshift_out
             }

    return coords

def convert_forthills_xy_to_lon_lat(x: float, y: float) -> Dict[str, float]:
    """
    Convert Forthills x y coordinates to longitude and latitude.
    
    Args:
        x: x-coordinate in FH Mine UTM.
        y: y-coordinate in FH Mine UTM.
    
    Returns:
        dict: A dictionary containing the converted longitude and latitude.
    """

    Scale_in = 0.0148785877482567
    Scale_out = 0.00110254550000008
    X1 = 0.223066936957123
    Xshift_in = -465403.244
    Xshift_out = -111.575445215
    Xx = 1.013444271818
    Xx2 = 1.75788830492074E-07
    Xxy = 0.000016629698096432
    Xy = -0.0085683713234209
    Xy2 = -1.64704307442642E-07
    Y1 = 0.0147910412223914
    Yshift_in = -6358439.502
    Yshift_out = 57.367463405
    Yx = 0.00462941131342801
    Yx2 = -4.50165392765906E-06
    Yxy = 1.1164954408921E-07
    Yy = 0.547569477883439
    Yy2 = -2.84681099249617E-08

    x = (x + Xshift_in) * Scale_in
    y = (y + Yshift_in) * Scale_in

    x_p = Xx * x + Xy * y + X1 + Xxy * x * y + Xx2 * x**2 + Xy2 * y**2
    y_p = Yx * x + Yy * y + Y1 + Yxy * x * y + Yx2 * x**2 + Yy2 * y**2

    coords = {'longitude': x_p * Scale_out + Xshift_out
            , 'latitude': y_p * Scale_out + Yshift_out
             }

    return coords
