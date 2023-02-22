#! /bin/bash

# Returns the ordered list of released AP versions with version > 5.8
curl -q -u "${ARTIFACTS_USER}:${ARTIFACTS_PASSWORD}" https://artifacts.activeviam.com/share/ActivePivot_stable/ | xmllint --html -xpath '///table/tr/td/a' - | grep -E '^.*[0-9]*\.[0-9]*\.[0-9]*.*$' - | cut -d '>'  -f2 | cut -d  '/' -f1  | sort -V - > all_versions.txt
while read line; do
	human_version=$(cut -d '.' -f1 <<< ${line})
	major_version=$(cut -d '.' -f2 <<< ${line})
    if [[ ( ${human_version} -ge 5 && ${major_version} -ge 8  ) || ${human_version} -ge 6 ]]; then
    	echo  ${line} >>compatible_versions.txt
    fi
done < all_versions.txt
cat compatible_versions.txt
rm -f compatible_versions.txt
rm -f all_versions.txt
