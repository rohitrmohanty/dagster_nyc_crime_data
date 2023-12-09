import sys
import subprocess
import pkg_resources

required = {'psycopg', 'pandas'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

def install():
    if missing:
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
        # process output with an API in the subprocess module:
        reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
        installed_packages = [r.decode().split('==')[0] for r in reqs.split()]
        print('Installed the following packages: \n')
        for package in installed_packages:
            print(package, '\n')
    else:
        print("All required packages are already installed! We are good to go..!!")


