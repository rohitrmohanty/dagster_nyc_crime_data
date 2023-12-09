import sys
import pkg_resources
import subprocess

import animation_script
import install_packages
import test_script

name = sys.argv[1]
print("Hello", name)

animation_script.animate()
install_packages.install()
test_script.bye(name)
