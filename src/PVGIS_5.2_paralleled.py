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
            processes = []

            line = inputs.readline()
            total_request_delay = 0
            while line:
                cicle_duration = time.time()
                p = m.Process(target=request_hourlydata, args=[line])
                p.start()
                processes.append(p)
                line = inputs.readline()
                sleep_duration = 0.03334-(time.time()-cicle_duration)
                if (sleep_duration>0):
                    total_request_delay += sleep_duration
                    time.sleep(sleep_duration)
            
            joining_time = time.time()
            for proc in processes:
                proc.join()
            total_request_delay += time.time()-joining_time

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs.dat")
