import sys, os, time, datetime, warnings, traceback, sqlite3
import geopandas as gpd
import shapely.wkb as swkb
from pandas.core.common import *
warnings.simplefilter(action="ignore", category= UserWarning)



banner = \
'''
     _             _____             _ _ _       
    | |           / __  \           | (_) |      
 ___| |__  _ __   `' / /'  ___  __ _| |_| |_ ___ 
/ __| '_ \| '_ \    / /   / __|/ _` | | | __/ _ \
\__ \ | | | |_) | ./ /___ \__ \ (_| | | | ||  __/
|___/_| |_| .__/  \_____/ |___/\__, |_|_|\__\___|
          | |                     | |            
          |_|                     |_|            
                             
                                  by Ilias Doukas
'''


def create_export_dir():
    now = datetime.datetime.now().strftime("%d-%b-%Y_%H-%M-%S")

    if getattr(sys, "frozen", False):
        currentDirName = os.path.dirname(sys.executable).replace('\\', '/')
    elif __file__:
        currentDirName = os.path.dirname(__file__).replace('\\', '/')

    dir = f"{currentDirName}/GeoNames_shp2sqlite_{now}"

    dirExists = os.path.isdir(dir)
    if not dirExists:
        os.mkdir(dir)

    return(currentDirName, dir)


def formatTime(processTime):
    if processTime > 60:
        processTime //= 60
        if processTime > 60:
            processTime /= 60
            processTime = f"{round(processTime, 1)} hours"
        else:
            processTime = f"{processTime} minutes"
    else:
        processTime = f"{processTime} seconds"
    return(processTime)


def toBinary(gdf):
    wkb = swkb.dumps(gdf.geometry)
    return(wkb)


def textGeometry(gdf):
    txt = {
        "x": gdf['geometry_x'],
        "y": gdf['geometry_y']
    }
    return(str(txt))



def main():
    """
    This tool exports geonames to .sqlite file from a given .shp file 
    and fills the AuGeo db's table.

    Usage:
    Place inside AuGeo directory the folder which contains AuGeo sqlite.
    Place geonames .shp file inside shp directory
    Creates a directory GeoNames_shp2sqlite_<datetime> where you can find the exported .sqlite file

    Arguments: 
    <imported_geonames_shapefile(.shp)>   <directory_of_AuGeo_sqlite>   (<exported_geonames_sqlite(.sqlite)>)

    Execution: 
    python geonames_shp2sqlite_v1.py shp/athens.shp athens
    """
    try:
        print(banner)
        
        if len(sys.argv) > 1:
            currentDir, exportDir = create_export_dir()

            shpFname = str(sys.argv[1]) if ".shp" in str(sys.argv[1]) else str(sys.argv[1]) + ".shp"
            shpPath = f"{currentDir}/shp/{shpFname}" 
            shpExists = os.path.isfile(shpPath)

            if shpExists:

                if len(sys.argv) > 2:
                    sqliteDir = f"{currentDir}/AuGeo/{sys.argv[2]}" 

                    if len(sys.argv) > 3:
                        datasetName = str(sys.argv[3]) if ".sqlite" in str(sys.argv[3]) else str(sys.argv[3]) + ".sqlite"
                    else:
                        datasetName = f"{shpFname.replace('.shp', '')}.sqlite"
                        
                    datasetPath = f"{exportDir}/{datasetName}" 

                    sqliteDirIsDir = os.path.isdir(sqliteDir)
                    if sqliteDirIsDir:
                        print("\n► Turn geonames .shp to .sqlite file... Please be patient. . .\n")
                        start = time.time()

                        sqliteDirList = os.listdir(sqliteDir)
                        sqliteFile = next(item for item in sqliteDirList if item.split(".")[1] == "sqlite")
                        sqliteFile = f"{sqliteDir}/{sqliteFile}"

                        
                        # Connect to AuGeo db
                        with sqlite3.connect(sqliteFile) as conn:
                            # Get the name of AuGeo db's table
                            cur = conn.execute(
                                """
                                    SELECT * FROM sqlite_master
                                    WHERE type='table'
                                """
                            )
                            tableName = cur.fetchall()[0][1]
                            print("Table Name: ", tableName)

                            # Get all data and columns
                            cur = conn.execute(
                                f"""
                                    SELECT * FROM {tableName}
                                """
                            )
                            cols = [member[0] for member in cur.description if member[0] != "FID"]
                            augeoInitData = cur.fetchall()
                            columns = ",".join(cols)
                            allColumns = "FID," + ",".join(cols)

                            print("Number of Columns: ", len(cols) + 1)
                            print("Columns: ", allColumns)

                            # Delete content of AuGeo db's table
                            cur = conn.execute(
                                f"""
                                    DELETE FROM {tableName}
                                """
                            )

                            # Check if items from AuGeo table was deleted successfully
                            cur = conn.execute(
                                f"""
                                    SELECT * FROM {tableName}
                                """
                            )
                            augeoEmptyData = cur.fetchall()
                            print("Data of AuGeo Table after deleting: ", augeoEmptyData)


                            # Read .shp file which was uploaded to ArcGIS Online (#AuGeo)
                            gdf = gpd.GeoDataFrame.from_file(shpPath)

                            df = gdf.drop(['geometry'], axis=1)
                            df['geometry_x'] = gdf.geometry.x
                            df['geometry_y'] = gdf.geometry.y
                            df['geometry'] = df.apply(textGeometry, axis= 1)

                            # Convert geometry to WKB in order to be used as a spatialite
                            # and create sql depending on the existance of UFI attribute
                            sqlTxt = "geonames"
                            if "UFI" in gdf.columns:
                                ufi = gdf['UFI'].to_list()
                                sqlTxt += ".UFI"
                            else:
                                ufi = gdf['id'].to_list()
                                sqlTxt += ".id"

                            gdf['wkb'] = gdf.apply(toBinary, axis= 1)
                            wkb = gdf['wkb'].to_list()

                            rows = tuple(zip(wkb, ufi))

                            # Connect - create geonames sqlite db
                            with sqlite3.connect(datasetPath) as conn2:
                                # Create table geonames to exported .sqlite db
                                # and populate it with non-geospatial data
                                # This is a different db from the AuGeo db
                                df.to_sql("geonames", conn2, if_exists= "replace", index= False)
                                conn2.enable_load_extension(True)
                                conn2.load_extension("mod_spatialite")
                                conn2.execute("SELECT InitSpatialMetaData(1);")

                                # Create column wkb_geometry where the geometry data will be stored
                                conn2.execute(
                                    """
                                        SELECT AddGeometryColumn('geonames', 'wkb_geometry', 4326, 'POINT', 2);
                                    """
                                )

                                # Update geometry column
                                conn2.executemany(
                                    f"""
                                        UPDATE geonames
                                        SET wkb_geometry=GeomFromWKB(?, 4326)
                                        WHERE {sqlTxt} = ?
                                    """, (rows)
                                )

                                # # Check if geometry was added successfully 
                                # cur = conn2.execute(
                                #     """
                                #     SELECT wkb_geometry FROM geonames
                                #     """
                                # )
                                # result = cur.fetchall()
                                # print(result)


                                # Get all data from generated table geonames
                                cur2 = conn2.execute(
                                    f"""
                                        SELECT {columns} FROM geonames
                                    """
                                )
                                data = cur2.fetchall()

                                # Add fid to geonames data
                                fid = [l for l in range(1, len(data) + 1)]
                                data = [(fid[l],) + data[l] for l in range(len(data))]
                                
                                # Create (?,?,..,?) wildcard for sql
                                colnamesTup = tuple("?" for l in range(len(cols) + 1))
                                colnames = "(" + ",".join(colnamesTup) + ")"

                                # Insert data from geonames table to AuGeo db table
                                conn.executemany(
                                    f"""
                                        INSERT INTO {tableName} VALUES {colnames}
                                    """, (data)
                                )

                                # # Check if data was successfully added to AuGeo db
                                # cur = conn.execute(
                                #     f"""
                                #         SELECT * FROM {tableName}
                                #     """
                                # )
                                # augeoFinalData = cur.fetchall()
                                # print("Final AuGeo db data: ", augeoFinalData)

                                end = time.time()

                                processTime = round(end - start, 1)
                                processTime = formatTime(processTime)

                                print(f"\n✓ Geonames {datasetName} and AuGeo db have exported successfully after {processTime}\n")
                                errorExists = os.path.isfile(f"{exportDir}/errors.log")
                                if errorExists:
                                    print(f"- For more details please open errors.log file.\n")

                    else:
                        print("✕ There is no such folder inside AuGeo directory.")

                else:
                    print("✕ Please input the name of AuGeo sqlite folder.")
            
            else:
                print("✕ There is no such .shp file.")

        else:
            print("✕ Please input the right arguments:\n<imported_geonames_shapefile(.shp)> <directory_of_AuGeo_sqlite> (<exported_geonames_sqlite(.sqlite)>)")

        # Delete exported directory if is empty
        exportDirItems = os.listdir(exportDir)
        if not exportDirItems:
            os.rmdir(exportDir)
            
        os._exit(0)

    except Exception as error:
        now = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        print(f"\n✕ Failed to execute:\n{traceback.format_exc()}\n")
        print(f"- For more details please open errors.log file.\n")
        with open(f"{exportDir}/errors.log", "a", encoding="utf-8") as file:
            file.write(f"✕ {now}: {traceback.format_exc()}\n")
        os._exit(1)


if __name__ == "__main__":
    main()
