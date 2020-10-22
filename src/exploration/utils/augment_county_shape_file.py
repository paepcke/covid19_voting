'''
Created on Oct 21, 2020

@author: paepcke
'''

import os,sys
from zipfile import ZipFile
import dbf
import tempfile
import shutil

class ShapeFileAugmenter(object):
    '''
    classdocs
    '''

    #------------------------------------
    # Constructor
    #-------------------

    def __init__(self, county_shape_file, outfile):
        '''
        
        Constructor
        /tmp/US_County_Shapfile.DBF/US_County_Shapfile.DBF
        '''

        data_dir = os.path.dirname(county_shape_file)
        # Grab the .DBF attributes file from within the shapefile zip:
        with ZipFile(county_shape_file) as county_shapes_zip:
            
            shapefile_names = county_shapes_zip.namelist()
            try:
                dbf_zipfname = next(fname for fname in shapefile_names if fname.lower().endswith('.dbf'))
            except StopIteration:
                raise ValueError(f"No .dbf file in {county_shape_file}.")
            
            with tempfile.TemporaryDirectory(suffix=".dbf", 
                                             prefix=dbf_zipfname, 
                                             dir=data_dir) as tmpdir_name:
                # Extract the dbf file to the tmp dir:
                dbf_fname = county_shapes_zip.extract(dbf_zipfname, path=tmpdir_name)

                tbl = dbf.Table(dbf_fname)
                
                # Did we already add the FULL_FIPS column?
                try:
                    tbl.field_names.index('FULL_FIPS')
                    print(f"Attribute file {dbf_fname} in zip archive {county_shape_file} already contains a FULL_FIPS column.")
                    sys.exit
                except ValueError:
                    # Good, it's not already there:
                    pass
                
                tbl.open(mode=dbf.READ_WRITE)
                
                # County FIPS are two chars: 2 state, 3 county:
                tbl.add_fields('FULL_FIPS C(5)')
                
                for record in tbl:
                    with record as r:
                        r.FULL_FIPS = f"{str(r.STATEFP).zfill(2) + str(r.COUNTYFP).zfill(3)}"
                        
                tbl.close()
                
                #****shutil.copyfile(county_shape_file, outfile)
                with ZipFile(outfile, mode='w') as new_county_shapes_zip:
                    # Write the new attributes file:
                    new_county_shapes_zip.write(dbf_fname, arcname=dbf_zipfname)
                    # Copy the other files from the old to the new:
                    zipped_files = county_shapes_zip.namelist()
                    for zipped_fname in zipped_files:
                        if not zipped_fname.lower().endswith('.dbf'):
                            with tempfile.TemporaryDirectory(suffix=".dbf", 
                                                             prefix=zipped_fname,
                                                             dir=data_dir) as tmp_file:
                                one_file = county_shapes_zip.extract(zipped_fname, path=tmp_file)
                                new_county_shapes_zip.write(one_file, arcname=zipped_fname)

                
# ----------------------------

if __name__ == '__main__':
    
    county_shape_file = os.path.join(os.path.dirname(__file__), 
                                     '../../../data/Exploration/us-county-shapefile1.zip')
    new_county_shape_file = os.path.join(os.path.dirname(__file__), 
                                         '../../../data/Exploration/us-county-shapefile_augmented.zip')

    print("Adding column 'FULL_FIPS'...")
    ShapeFileAugmenter(county_shape_file, new_county_shape_file)
    print("Done adding column 'FULL_FIPS'")
