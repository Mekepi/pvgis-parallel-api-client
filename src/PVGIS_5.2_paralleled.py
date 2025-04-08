import requests
import multiprocessing as m
import concurrent.futures as cf
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

            total_request_delay = time.time()
            with cf.ProcessPoolExecutor() as executor:
                j = 0
                for i in range(0,len(inputs.readlines()),30):
                    start_requests = time.time()
                    print(inputs.readlines()[1])
                    executor.submit(request_hourlydata, inputs.readlines()[j:i+1])
                    j=i+1
                    time.sleep(1-(time.time()-start_requests))
            total_request_delay = time.time()-total_request_delay

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs.dat")
