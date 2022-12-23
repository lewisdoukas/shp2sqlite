# shp2sqlite
This tool exports geonames to .sqlite file from a given .shp file and fills the AuGeo db's table.

# Installation
Python version 3.8.5 >= is required.  

`pip3 install -r requirements.txt`  

# Usage
Place inside AuGeo directory the folder which contains AuGeo sqlite.  
Place geonames .shp file inside shp directory.  


# Arguments
`<imported_geonames_shapefile(.shp)>  <directory_of_AuGeo_sqlite>  (<exported_geonames_sqlite(.sqlite)>)`  

# Execution
`python geonames_shp2sqlite.py athens.shp athens`  
  
# Output
The tool creates a directory directory GeoNames_shp2sqlite_<datetime> where you can find the exported .sqlite file.. 
