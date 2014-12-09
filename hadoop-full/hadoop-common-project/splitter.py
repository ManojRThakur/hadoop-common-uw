#!/usr/bin/python

import os
import sys

afu = os.environ["AFU_HOME"] + '/annotation-file-utilities/scripts/insert-annotations-to-source' 
def main():

    if len(sys.argv) == 1:
        print 'Using file solved.jaif'
        infile = 'solved.jaif'
    else:
        infile = sys.argv[1]

    header = """
package ostrusted.quals:
annotation @OsUntrusted:
package ostrusted.quals:
annotation @OsTrusted:

"""
    package_entries = {}
    with open(infile) as f:
        lines = f.readlines()

    entry = []
    duplicates = {}
    for line in lines:
        if line == '\n':
            if len(entry) > 0:
                package_name = entry[0].partition(' ')[2]
                joined = ''.join(entry).replace('\n','')
                if joined not in duplicates:
                    duplicates[joined] = True
                else:
                    entry = []
                    continue

                if package_entries.get(package_name):
                    package_entries[package_name] += entry
                else:
                    package_entries[package_name] = entry
                entry = []
        else:
            entry.append(line)

    with open('out_commands.sh', 'w') as out:
        for (package_name, value) in package_entries.iteritems():
            package_name = package_name.replace(':', '').replace('\n', '')
            file_path = 'cmds/' + package_name + '.jaif'
            package_path = 'src/main/java/' + package_name.replace('.', "/")
            exec_line = "%s -i %s ` find %s -iname '*.java' -maxdepth 1`" % (afu, file_path, package_path )
            print exec_line
            out.write(exec_line)
            out.write('\n')
            with open(file_path, 'a') as afu_out:
                afu_out.write(header)
                afu_out.write('\n')
                afu_out.write(''.join(value))
                afu_out.write('\n')

if __name__=='__main__':
    main()
