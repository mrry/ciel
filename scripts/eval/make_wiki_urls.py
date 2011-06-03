prefix = 'http://mrry.s3.amazonaws.com/wikipedia-full-parsed.'

#for n in range(ord('a'), ord('z')+1):
#    print prefix + chr(n)

for m in range(ord('a'), ord('f')+1):
    for n in range(ord('a'), ord('z')+1):
        print "%s%s%s" % (prefix, chr(m), chr(n))

for n in range(ord('a'), ord('i')+1):
    print "%sg%s" % (prefix, chr(n))
