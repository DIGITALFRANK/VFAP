import semantic_version

# M - Major
# m - Minor
# p - patch

version = semantic_version.Version('0.1.1')
increment = "M"
if increment == "M":
	new_version = version.next_major()
	print(str(new_version))
elif increment == "m":
	new_version = version.next_minor()
	print(str(new_version))
elif increment == "p":
	new_version = version.next_patch()
	print(str(new_version))
