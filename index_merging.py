import sys
import bz2
import heapq
import os
import operator
from collections import defaultdict
import threading
import re


filepath = "./index/index_"
map_field = {'t':0 , 'x':1 , 'i':2 , 'l':3 , 'c':4 , 'r':5 }
comp_filecount = -1

title_offset_list = []
text_offset_list = []
info_offset_list = []
link_offset_list = []
cat_offset_list = []
ref_offset_list = []

title_offoff_list = []
text_offoff_list = []
info_offoff_list = []
link_offoff_list = []
cat_offoff_list = []
ref_offoff_list = []


title_offset_offset = 0
text_offset_offset = 0
info_offset_offset = 0
link_offset_offset = 0
cat_offset_offset = 0
ref_offset_offset = 0

class writeParallel(threading.Thread):

    def __init__(self, filename, fileno,field_list):
        threading.Thread.__init__(self)
        self.filename = filename
        self.fileno = fileno
        self.data = field_list

    def run(self):
        fname = self.filename + str(self.fileno) + ".txt.bz2"
        with bz2.BZ2File(fname, 'wb', compresslevel=9) as f:
            f.write('\n'.join(self.data))


def copy_files(one_word):

    global comp_filecount
    comp_filecount += 1

    global title_offset_offset
    global text_offset_offset
    global info_offset_offset
    global link_offset_offset
    global cat_offset_offset
    global ref_offset_offset

    alag_title = []
    alag_text = []
    alag_info = []
    alag_link = []
    alag_cat = []
    alag_ref = []

    title_list = []
    text_list = []
    info_list = []
    link_list = []
    cat_list = []
    ref_list = []

    title_offset = 0
    text_offset = 0
    info_offset = 0
    link_offset = 0
    cat_offset = 0
    ref_offset = 0

    for word in sorted(one_word.iterkeys()):
        five = {}
        for record in one_word[word]:
            docs = record.split('|')
            for doc in docs:
                count = re.split(r'(\d+)',doc)
                docid = count[1]
                for entry in range(2,len(count),2):
                    if count[entry] != '':
                        field = count[entry]
                        field_count = count[entry+1]
                        if (docid in five):
                            five[docid][map_field[field]] += int(field_count)
                        else:
                            five[docid] = [0,0,0,0,0,0]
                            five[docid][map_field[field]] += int(field_count)

        title_string = word + " "
        text_string = word + " "
        info_string = word + " "
        link_string = word + " "
        cat_string = word + " "
        ref_string = word + " "
        flag = [0]*6


        for docid in sorted(five.iterkeys()):
            if five[docid][0] > 0:
                title_string += str(docid) + '_' + str(five[docid][0]) + '|'
                flag[0] = 1
            if five[docid][1] > 0:
                text_string += str(docid) + '_' + str(five[docid][1]) + '|'
                flag[1] = 1
            if five[docid][2] > 0:
                info_string += str(docid) + '_' + str(five[docid][2]) + '|'
                flag[2] = 1
            if five[docid][3] > 0:
                link_string += str(docid) + '_' + str(five[docid][3]) + '|'
                flag[3] = 1
            if five[docid][4] > 0:
                cat_string += str(docid) + '_' + str(five[docid][4]) + '|'
                flag[4] = 1
            if five[docid][5] > 0:
                ref_string += str(docid) + '_' + str(five[docid][5]) + '|'
                flag[5] = 1



        if flag[0]:
            title_list.append(title_string[:-1])
            title_string = title_string[:-1]
            title_string += '\n'
            alag_title.append(str(title_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(title_offset)
            title_offset += len(title_string.encode('utf-8'))
            title_offset_list.append(newstring)
            title_offoff_list.append(str(title_offset_offset))
            newstring += '\n'
            title_offset_offset += len(newstring.encode('utf-8'))


        if flag[1]:
            text_list.append(text_string[:-1])
            text_string = text_string[:-1]
            text_string += '\n'
            alag_text.append(str(text_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(text_offset)
            text_offset += len(text_string.encode('utf-8'))
            text_offset_list.append(newstring)
            text_offoff_list.append(str(text_offset_offset))
            newstring += '\n'
            text_offset_offset += len(newstring.encode('utf-8'))
        if flag[2]:
            info_list.append(info_string[:-1])
            info_string = info_string[:-1]
            info_string += '\n'
            alag_info.append(str(info_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(info_offset)
            info_offset += len(info_string.encode('utf-8'))
            info_offset_list.append(newstring)
            info_offoff_list.append(str(info_offset_offset))
            newstring += '\n'
            info_offset_offset += len(newstring.encode('utf-8'))
        if flag[3]:
            link_list.append(link_string[:-1])
            link_string = link_string[:-1]
            link_string += '\n'
            alag_link.append(str(link_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(link_offset)
            link_offset += len(link_string.encode('utf-8'))
            link_offset_list.append(newstring)
            link_offoff_list.append(str(link_offset_offset))
            newstring += '\n'
            link_offset_offset += len(newstring.encode('utf-8'))
        if flag[4]:
            cat_list.append(cat_string[:-1])
            cat_string = cat_string[:-1]
            cat_string += '\n'
            alag_cat.append(str(cat_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(cat_offset)
            cat_offset += len(cat_string.encode('utf-8'))
            cat_offset_list.append(newstring)
            cat_offoff_list.append(str(cat_offset_offset))
            newstring += '\n'
            cat_offset_offset += len(newstring.encode('utf-8'))
        if flag[5]:
            ref_list.append(ref_string[:-1])
            ref_string = ref_string[:-1]
            ref_string += '\n'
            alag_ref.append(str(ref_offset))
            newstring = word + " " + str(comp_filecount) #+ " " + str(ref_offset)
            ref_offset += len(ref_string.encode('utf-8'))
            ref_offset_list.append(newstring)
            ref_offoff_list.append(str(ref_offset_offset))
            newstring += '\n'
            ref_offset_offset += len(newstring.encode('utf-8'))


    thread1 = writeParallel("./index/merged_index_title",comp_filecount,title_list)
    thread2 = writeParallel("./index/merged_index_text",comp_filecount,text_list)
    thread3 = writeParallel("./index/merged_index_info",comp_filecount,info_list)
    thread4 = writeParallel("./index/merged_index_link",comp_filecount,link_list)
    thread5 = writeParallel("./index/merged_index_cat",comp_filecount,cat_list)
    thread6 = writeParallel("./index/merged_index_ref",comp_filecount,ref_list)

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()

    thread1 = writeParallel("./index/merged_offset_title",comp_filecount,alag_title)
    thread2 = writeParallel("./index/merged_offset_text",comp_filecount,alag_text)
    thread3 = writeParallel("./index/merged_offset_info",comp_filecount,alag_info)
    thread4 = writeParallel("./index/merged_offset_link",comp_filecount,alag_link)
    thread5 = writeParallel("./index/merged_offset_cat",comp_filecount,alag_cat)
    thread6 = writeParallel("./index/merged_offset_ref",comp_filecount,alag_ref)

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()

def merge_files():
    filecount = len(os.listdir('./index/'))
    file_dis = {}
    wordline = {}
    flag = [0]*filecount
    queue = []
    one_word = defaultdict(list)
    for i in xrange(filecount):
        fileName = filepath + str(i) + '.txt.bz2'
        file_dis[i] = bz2.BZ2File(fileName, 'rb')
        flag[i] = 1
        wordline[i] = file_dis[i].readline().split()
        if wordline[i][0] not in queue:
            heapq.heappush(queue, wordline[i][0])

    while any(flag) == 1:
        word = heapq.heappop(queue)
        for i in xrange(filecount):
            if (flag[i] == 1 and wordline[i][0] == word):
                one_word[word].append(wordline[i][1])
                wordline[i] = file_dis[i].readline().split()
                if not wordline[i]:
                    flag[i] = 0
                    file_dis[i].close()
                    os.remove(filepath + str(i) + '.txt.bz2')
                elif wordline[i][0] not in queue:
                        heapq.heappush(queue, wordline[i][0])
        if sys.getsizeof(one_word) > 100000:
            copy_files(one_word)
            one_word.clear()
    if bool(one_word):
        copy_files(one_word)



    with bz2.BZ2File("./index/offset_title.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(title_offset_list))
    with bz2.BZ2File("./index/offset_text.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(text_offset_list))
    with bz2.BZ2File("./index/offset_info.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(info_offset_list))
    with bz2.BZ2File("./index/offset_link.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(link_offset_list))
    with bz2.BZ2File("./index/offset_cat.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(cat_offset_list))
    with bz2.BZ2File("./index/offset_ref.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(ref_offset_list))

    with bz2.BZ2File("./index/offoff_title.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(title_offoff_list))
    with bz2.BZ2File("./index/offoff_text.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(text_offoff_list))
    with bz2.BZ2File("./index/offoff_info.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(info_offoff_list))
    with bz2.BZ2File("./index/offoff_link.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(link_offoff_list))
    with bz2.BZ2File("./index/offoff_cat.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(cat_offoff_list))
    with bz2.BZ2File("./index/offoff_ref.txt.bz2", 'wb', compresslevel=9) as f:
        f.write('\n'.join(ref_offoff_list))


if __name__ == "__main__":
    merge_files()

