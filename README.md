# Wind-degrib-transformer: North American Regional Reanalysis grib files converter

This repo contains all the necesary code to build a Docker container to compile and run National Weather Service `degrib` utility. [This is _WIP_]. Right now, this repository builds a Docker service that mounts a local volume in the container to use the `degrib` to CSV tool. 

Basic use:

After installing Docker in your local machine, start the infrastrucutre by:

```
docker-compose up
```

