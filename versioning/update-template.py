### This python script except two arguments.
###		argv[1] = new_version
###		argv[1] = template_path

import os
import sys

### This function is to update the version in template file
def update_version_inCF(new_version,template_path):
	if template_path != "NULL":
		with open(template_path, 'r+') as file:
			new_template=file.read().replace('###version###', new_version)
		with open(template_path, "w") as file:
			file.write(new_template)

if __name__ == '__main__':
	### Read input arguments
	template_path = str(sys.argv[1])
	new_version = str(sys.argv[2])
	update_version_inCF(new_version,template_path)