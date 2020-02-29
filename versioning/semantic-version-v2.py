### semantic_version program expects three arguments
# Arguments:
# 	argv[1] = current_version
#	argv[2] = update_type
#
# Update type:
# 	M - Major
# 	m - Minor
# 	p - patch
#
# Dependencies: Should install semantic_version using command "pip install semantic_version"

import semantic_version
import os
import sys

### This function is to get next version based on update_type
def get_version(current_version,update_type):
	### Initialize the semantic_version
	version = semantic_version.Version(current_version)
	
	if update_type == "M":
		new_version = version.next_major()
	elif update_type == "m":
		new_version = version.next_minor()
	elif update_type == "p":
		new_version = version.next_patch()
	else:
		print("[ERROR]: Vaild update type is M - major, m - minor and p - patch")
	
	return new_version

if __name__ == '__main__':
	### Read input arguments
	current_version = str(sys.argv[1])
	update_type = str(sys.argv[2])
	new_version=get_version(current_version,update_type)