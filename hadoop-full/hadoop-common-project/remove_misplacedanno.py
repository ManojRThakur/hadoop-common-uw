#!/usr/bin/env python

import sys
import re

class JavaError:
    def __init__(self, filename, line):
        self.filename = filename
        self.line = line

    def __repr__(self):
        return "%s:%d" % (self.filename, self.line)

def main():
    lines = sys.stdin.readlines()
    error_pattern = re.compile('^(.+\.java):([0-9]+).*$')
    errors_to_fix = []
    for line in lines:
        match = error_pattern.match(line)
        if match:
            filename = match.group(1)
            line = int(match.group(2)) -1
            errors_to_fix.append(JavaError(filename, line))

    for error in errors_to_fix:
        fixup_error(error)

def fixup_error(java_error):
    with open(java_error.filename) as f:
        lines = f.readlines()

    lines = fix_class_anno(lines, java_error)
    with open(java_error.filename, 'w') as f:
        f.write(''.join(lines))

def fix_class_anno(lines, java_error):
    line = lines[java_error.line]
    print "Fixing line:" + line
    updated_line = re.sub("Class<\?.+>", "Class<?>", line)
    lines[java_error.line] = updated_line
    return lines


if __name__=='__main__':
    main()
