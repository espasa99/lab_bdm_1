# this is a simulation for the collector of idealista

from collectors_funcs import register_upload

# We simulate that the files from 2017 and 2016 have been uploaded to the temporal_landing folder.
files_to_upload = ['2021_03_09_idealista.json', '2021_02_25_idealista.json']

for file_name in files_to_upload:

    date = file_name.split('_')[0]

    # We save a register of the uploads to be able to know which files have been uploaded.
    register_upload(date, file_name, 'json', 'idealista')