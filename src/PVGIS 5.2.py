import requests
import time

def pvgis_5_2_hourlydata(file):
    start_time = time.time()
    total_request_time = 0
    try:
        inputs = open(file, "r")
    except FileNotFoundError as err:
        print(err)
    else:
        line = inputs.readline()
        while line:
            [lat,lon,startyear,endyear] = line.split(",")
            [lat,lon,startyear,endyear] = [float(lat),float(lon),int(startyear),int(endyear)]
            start_request_time = time.time()
            response = requests.get("https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(lat,lon,startyear,endyear))
            total_request_time += time.time()-start_request_time
            try:
                f = open("hourlydata(%f,%f)[%i,%i].csv"%(lat,lon,startyear,endyear), "x")
            except FileExistsError as err:
                print(err)
            else:
                f.write(response.text)
                f.close()
            line = inputs.readline()
    print("total request time: %f\nexecution time: %f" %(total_request_time, time.time()-start_time))
    """ou seja, o request é a maior parte do tempo de execução"""
            
pvgis_5_2_hourlydata("inputs.dat")