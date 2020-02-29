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

### Read input arguments
current_version = str(sys.argv[1])
update_type = str(sys.argv[2])

print(current_version)
print(update_type)

version = semantic_version.Version(current_version)

if update_type == "M":
	new_version = version.next_major()
elif update_type == "m":
	new_version = version.next_minor()
elif update_type == "p":
	new_version = version.next_patch()
else:
	print("[ERROR]: Vaild update type is M - major, m - minor and p - patch")

print(str(new_version))