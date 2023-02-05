import os
import sys

ta_lib_pip_details = os.system("python3 -m pip show Flask")
if ta_lib_pip_details == 0:
    print("Flask already installed")
else:
    if os.system("python3 -m pip install Flask") != 0:
        print("Failed to pip install Flask")
        sys.exit(1)

    print("Installed Flask pip package")