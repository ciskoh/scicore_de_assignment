"""
installs all dependencies to tun the code
"""

import subprocess

subprocess.run(["pip", "install" ,"-r", "requirements.txt"])

print("\n --- DEPENDECIES INSTALLED --- project is ready to run" )
print( " run exercise with 'python main.py -e {exercise number}'")
