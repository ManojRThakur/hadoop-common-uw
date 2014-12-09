#!/usr/bin/python

afu = '~/dev/dff_2/annotation-tools/annotation-file-utilities/scripts/insert-annotations-to-source' 
def main():
    infile = 'solved.jaif'
    header = """
package ostrusted.quals:
annotation @OsUntrusted:
package ostrusted.quals:
annotation @OsTrusted:

"""
    entries = {}
    with open(infile) as f:
        lines = f.readlines()

    entry = []
    for line in lines:
        if line == '\n':
            if len(entry) > 0:
                entry.append(line)
                if entries.get(entry[0].partition(' ')[2]):
                    entries[entry[0].partition(' ')[2]] += entry
                else:
                    entries[entry[0].partition(' ')[2]] = entry
                entry = []
        else:
            entry.append(line)

    with open('out_commands.sh', 'w') as out:
        for (key, value) in entries.iteritems():
            key = key.replace(':', '').replace('\n', '')
            file_path = 'cmds/' + key + '.jaif'
            package_path = 'src/main/java/' + key.replace('.', "/")
            exec_line = "%s -i %s ` find %s -maxdepth 1 -iname '*.java' ` " % (afu, file_path, package_path )
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
