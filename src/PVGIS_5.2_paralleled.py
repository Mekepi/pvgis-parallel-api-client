import requests
import multiprocessing as m
import numpy as np
import time

def request_hourlydata(line):
    [lat,lon,startyear,endyear] = line.split(",")
    [lat,lon,startyear,endyear] = [float(lat),float(lon),int(startyear),int(endyear)]
    request_start = time.monotonic()
    response = requests.get("https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(lat,lon,startyear,endyear))
    print("Request duration:", time.monotonic()-request_start)
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
            v_process = np.vectorize(lambda line: m.Process(target=request_hourlydata, args=[line]), [type(m.Process())])
            coords = inputs.readlines()
            processes = np.array_split(np.array(v_process(np.array([coords])))[0], np.ceil(len(coords)/m.cpu_count()))
            #print(processes)

            total_request_delay = 0
            joining_duration = 0
            chunk_rest = m.cpu_count()/30
            for processes_chunk in processes:
                sleep_duration = time.time()
                np.vectorize(lambda process: process.start(),otypes=[type(None)])(processes_chunk)
                """ joining_start =  time.time()
                np.vectorize(lambda process: process.join(),otypes=[type(None)])(processes_chunk)
                joining_duration += time.time()-joining_start """
                sleep_duration = chunk_rest-(time.time()-sleep_duration)
                if (sleep_duration>0): 
                    time.sleep(sleep_duration)
                    total_request_delay += sleep_duration
            print("  Sleep duration:", total_request_delay)

            joining_duration =  time.time()
            for process_chunk in processes:
                for process in process_chunk:
                    process.join()
            joining_duration = time.time()-joining_duration
            print("Joining duration:", joining_duration)
            total_request_delay += joining_duration

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs2.dat")
