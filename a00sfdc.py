from sfpull import sfpullfunc_flat as sfflat
from sfpull import sfpullfunc as sf
from mapfuncs import mapfuncsfuncs as mf
from obiee import obiee_conn
from obiee import obiee_conn_flat as obiee_flat
import json
import pandas as pd
import time

#write files to use flat later

sfresults = sf.sffunc()