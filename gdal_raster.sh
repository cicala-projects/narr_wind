#bin/bash

PWD=$(pwd)
PATH_IN_S3=$1
PATH_TO_DOWNLOAD=$2

# Create temp dir
if [[ ! -d $PWD/temp_data ]]
then
    mkdir $PWD/temp_data
fi

# Create output dir
if [[ ! -d $PWD/$2 ]]
then
    mkdir $PWD/$2
fi

# Use AWS CLI to get data
aws s3 cp $PATH_IN_S3 $PATH_TO_DOWNLOAD 

# Unzip downloaded zip
unzip $PATH_TO_DOWNLOAD -d unzip_raw_data

# Run gdal_transform
find ./unzip_raw_data -name '*.grb' -type f | parallel -- gdal_translate -of GTiff -b 293 -b 294 {} $PWD/$2/{/.}_raster.tif

# Delete temp dir
echo 'Deleting temporary data directory'
rm -r $PWD/temp_data

