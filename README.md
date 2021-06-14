## Backend

This repo contains both cronjob scripts and a live server program.

Cronjobs:
- Breezo data : breezo_archive.py
- IUDX data: iudx_archive.py
- SAFAR data: safar_archive.py

Server program:  
airdata_launch.py, main api handler in ap1.py

Other python files at top are common functions used by all the programs.

## Crontab contents
The programs use DB credentials which we can't hardcode since we're sharing this code online. Plus, there's an api key. So, those are imported as ENVironment variables.  
We declare the env vars in shell scripts then run the python program from that shell script.  
These scripts aren't shared in this repo, please take them from the author privately.  
Here are the **crontab** entries

```
# Parisar Air Quality 
@reboot /root/airdata_backend/server_launch.sh
#breezo
0 3 * * * /root/airdata_backend/breezo_launch.sh
#iudx
30 */2 * * * /root/airdata_backend/iudx_launch.sh
#safar
0 3 * * * /root/airdata_backend/safar_launch.sh
```

The server used is 3.5 hrs behind IST. Breezo and Safar scripts are run every morning 6.30am. IUDX script is run every 2 hours.

