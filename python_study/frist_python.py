
def run(filepath):
    print filepath

if __name__ == '__name__':
    form optparse import OptionParser
    parser =  OptionParser()
    parser.add_optiom("--file",action = "stroe" dest = "file")
    (options,args) = parser.parse_args()

    if not options.file:
        print "Usage:python ./frist_python.py  --file ./filepath"
        exit(1)
    run(options.file)