####################
#To compare two md5 files more quickly than compare_md5files.py
#
#A. Waterfall 15/12/2015
####################

def compare_md5files_quick(file1,file2):
    
    from collections import Counter

    #----------------------
    #read the first file
    f = open(file1,'r')
    data = f.readlines()
    
    filenames_1=[x.split()[1] for x in data]
    basenames_1 = [x.split('/')[-1] for x in filenames_1]
    cksum_bname_1 = [x.split()[0] +' '+ x.split('/')[-1] for x in data]

    
    #check for any duplicate filenames
    count = Counter()
    for name in basenames_1:
        count[name]+=1
    print('Duplicate files in first list:',[key for key,value in count.items() if value > 1] ) 

    #convert to sets to do matching
    set_1 = set(cksum_bname_1)
    bset_1 = set(basenames_1)


    #-----------------------
    #read the second file
    f2 = open(file2,'r')
    data2 = f2.readlines()


    filenames_2 = [x.split()[1] for x in data2]
    basenames_2 = [x.split('/')[-1] for x in filenames_2]
    cksum_bname_2 = [x.split()[0] +' '+ x.split('/')[-1] for x in data2]

  
    set_2 = set(cksum_bname_2)
    bset_2 = set(basenames_2)


   #check for any duplicate filenames
    count2 = Counter()
    for name in basenames_2:
        count2[name]+=1
    print('Duplicate files in second list:',[key for key,value in count2.items() if value > 1] ) 


    # ---------------------
    #use sets to work out matches / non-matches

    matches = set_1.intersection(set_2)

    #missing files
    missing1_2 = bset_1.difference(bset_2)
    missing2_1 = bset_2.difference(bset_1)

    #non-matching checksums
    diff1_2 = set_1.difference(set_2)   #files in dataset 1 byut not dataset2
    diff2_1 = set_2.difference(set_1)


    #-----------------------
    #check the checksums

    # print out results
    print('----------')
    print('Results')
    print('----------')
    print('No of files in first list: ', len(data))
    print('No of files in second list: ', len(data2))
    print('Missing files in first list',*sorted(missing2_1),sep='\n')
    print('Missing files in second list',*sorted(missing1_2),sep='\n')
    print('-----')
    print('No of non-matching files from dataset 1: ', len(diff2_1))
    print(*sorted([x.split()[-1] for x in diff2_1 if x.split()[-1] not in missing2_1]),sep='\n')
    print('-----')
    print('No of non-matching files from dataset 2: ', len(diff1_2))
#   print sorted([x.split()[-1] for x in diff1_2 if x.split()[-1] not in missing1_2])

    return
