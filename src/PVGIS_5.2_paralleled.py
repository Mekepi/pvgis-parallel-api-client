import requests
import multiprocessing as m
import time

def request_hourlydata(line):
    [lat,lon,startyear,endyear] = line.split(",")
    [lat,lon,startyear,endyear] = [float(lat),float(lon),int(startyear),int(endyear)]
    response = requests.get("https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(lat,lon,startyear,endyear))
    try:
        f = open("hourlydata(%f,%f)[%i,%i].csv"%(lat,lon,startyear,endyear), "x")
    except FileExistsError as err:
        print(err)
    else:
        f.write(response.text)
        f.close()

def pvgis_5_2_hourlydata(file):
    
    if __name__ == '__main__':

        start_time = time.time()

        try:
            inputs = open(file, "r")

        except FileNotFoundError as err:
            print(err)

        else:

            m.Pool(m.cpu_count()).map_async(request_hourlydata, inputs.readlines()).wait()

        print("execution time: %f" %(time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs2.dat")
