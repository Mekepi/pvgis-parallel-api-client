import urllib3 as u
import multiprocessing as m
import numpy as np
import time

def request_hourlydata(line):
    line = line.split(",")
    line = [float(line[0]),float(line[1]),int(line[2]),int(line[3])]
    try:
        f = open("hourlydata(%f,%f)[%i,%i].csv"%(line[0],line[1],line[2],line[3]), "x")
    except FileExistsError as err:
        print(err)
    else:
        request_start = time.monotonic()
        f.write(u.request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(line[0],line[1],line[2],line[3]),preload_content=False,timeout=None).data)
        print("Request", m.current_process().name.rsplit("-")[1] ,"duration:", time.monotonic()-request_start)
        f.close()

def pvgis_5_2_hourlydata(file):
    
    if __name__ == '__main__':

        start_time = time.time()

        try:
            inputs = open(file, "r")

        except FileNotFoundError as err:
            print(err)

        else:
            c_process = np.vectorize(lambda line: m.Process(target=request_hourlydata, args=[line]))
            processes = np.array(c_process(np.array([inputs.readlines()])))
            
            i=0; j=0
            v_start = np.vectorize(lambda process: process.start(),otypes=[type(None)], cache=True)
            chunk_minimun_rest = (m.cpu_count()/30)*1.01
            total_request_delay = 0
            for i in range(m.cpu_count(), np.shape(processes)[1]+1, m.cpu_count()):
                v_start(processes[0,j:i])
                j=i
                time.sleep(1)
                total_request_delay += chunk_minimun_rest
            if (i<np.shape(processes)[1]):
                j=i
                i=np.shape(processes)[1]
                v_start(processes[0,j:i])

            print("  Sleep duration:", total_request_delay)
            print("   Until joining:", time.time()-start_time)
            joining_start = time.time()
            for process in processes[0]:
                process.join()
                process.close()
            print("Joining duration:", time.time()-joining_start)
            total_request_delay += time.time()-joining_start

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time.time()-start_time))
            
pvgis_5_2_hourlydata("inputs2.dat")
