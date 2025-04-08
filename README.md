# PVGIS Solar Radiation Parallel API Client  

Parallel fetcher for hourly solar radiation data from [PVGIS](https://re.jrc.ec.europa.eu/pvg_tools/en/#HR).

## Features  
- **Parallel processing** (100+ concurrent requests)  
- **Memory-aware throttling** (auto-pauses when RAM <300MB free)  
- **Gzip compression** (75% size reduction)  

## Usage  
```python
from src.api_client import city_timeseries
city_timeseries([3304557])  # Rio de Janeiro