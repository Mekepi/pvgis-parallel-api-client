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
            
            with cf.ProcessPoolExecutor() as executor:
                
                total_sleep = 0
                total_cycle_duration = 0
                line = inputs.readline()
                while line:
                    cicle_duration = time.time()
                    executor.submit(request_hourlydata, line)
                    line = inputs.readline()
                    total_cycle_duration += (time.time()-cicle_duration)
                    sleep_duration = 0.03334-(time.time()-cicle_duration)
                    if (sleep_duration>0):
                        total_sleep += sleep_duration
                        print("Sleeping %f"%(sleep_duration))
                        time.sleep(sleep_duration)

        print("total cycles duration: %f\ntotal sleep: %f\nexecution time: %f" %(total_cycle_duration, sleep_duration,time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs.dat")
