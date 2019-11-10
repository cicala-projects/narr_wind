#bin/bash

PWD=$(pwd)
PATH_IN_S3=$1
SAVE_PATH_NAME=$2
BANDS=$3

# Create temp dir
TEMP_DATA=$PWD/temp_data
if [[ ! -d $TEMP_DATA ]]
then
    mkdir $TEMP_DATA
fi

# Create output dir
OUTPUT_DATA=$PWD/$2
if [[ ! -d  $OUTPUT_DATA ]]
then
    mkdir $OUTPUT_DATA
fi

# Use AWS CLI to get data
ZIP_PATH=$TEMP_DATA/zip_file_temp.zip 
aws s3 cp $PATH_IN_S3 $ZIP_PATH 

# Unzip downloaded zip
unzip $ZIP_PATH -d $TEMP_DATA/unzip_raw_data

# Run gdal_transform
find $TEMP_DATA/unzip_raw_data -name '*.grb' -type f | parallel -- gdal_translate -of GTiff -b $3 {} $OUTPUT_DATA/{/.}_raster.tif

# Delete temp dir
echo 'Deleting temporary data directory'
rm -r $PWD/temp_data

