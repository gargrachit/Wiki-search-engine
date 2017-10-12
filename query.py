import bz2
import math
import re
import operator
from collections import defaultdict
from Stemmer import Stemmer
import datetime
stem=Stemmer('english')

class query():

    def __init__(self):
        self.net_score = {}
        self.total = 0
        self.field_flag = {'T':'0','B':'1','I':'2','L':'3','C':'4','R':'5'}
        self.field_map = {'0' : './index/merged_index_title' , '1':'./index/merged_index_text', '2':'./index/merged_index_info','3':'./index/merged_index_link','4':'./index/merged_index_cat','5':'./index/merged_index_ref'}
        self.fieldOff_map = {'0' : './index/merged_offset_title' , '1':'./index/merged_offset_text', '2':'./index/merged_offset_info','3':'./index/merged_offset_link','4':'./index/merged_offset_cat','5':'./index/merged_offset_ref'}
        self.offset_map = {'0' : './index/offset_title.txt.bz2' , '1' : './index/offset_text.txt.bz2', '2':'./index/offset_info.txt.bz2','3':'./index/offset_link.txt.bz2','4':'./index/offset_cat.txt.bz2','5':'./index/offset_ref.txt.bz2'}
        self.offoff_map = {'0' : './index/offoff_title.txt.bz2' , '1' : './index/offoff_text.txt.bz2', '2':'./index/offoff_info.txt.bz2','3':'./index/offoff_link.txt.bz2','4':'./index/offoff_cat.txt.bz2','5':'./index/offoff_ref.txt.bz2'}
        self.title_dict = {}
        self.text_dict = {}
        self.info_dict = {}
        self.link_dict = {}
        self.cat_dict = {}
        self.ref_dict = {}
        self.id_title = defaultdict(list)
        self.sw = {}

    def binSearch(self,offset_file,search_file,x):
        f = bz2.BZ2File(search_file,'rb')
        arr = self.load(offset_file)
        low = 0
        high = len(arr) - 1
        ans = -1
        while low <= high:
            mid = (low+high)/2
            offset = arr[mid]
            f.seek(int(offset),0)
            temp_word = f.readline().split(' ')[0]
            if temp_word == x:
                ans = int(offset)
                return ans
            elif temp_word < x:
                low = mid+1
            else:
                high = mid-1
        return ans

    def getStopwords(self):
        f=open('stopwordsfile.txt', 'r')
        stopwords=[line.rstrip() for line in f]
        self.sw=dict.fromkeys(stopwords)
        f.close()

    def tokenize(self,line):
        line = line.lower()
        newline = re.sub('[^a-z]',' ', line)
        line = newline.split()
        line = [key.encode('utf-8') for key in line]
        line = [x for x in line if x not in self.sw]
        tokens = [stem.stemWord(word) for word in line]
        return tokens

    def preprocess(self):
        self.total = self.get_total()
        self.id_title = self.get_titles()
        self.getStopwords()

    def get_titles(self):
        temp_dict = defaultdict(list)
        with open('./id_title.txt','rb') as f:
            for line in f:
                row =  line.split(' ')
                iD = str(row[0])
                title = []
                for i in range(1,len(row)):
                    title.append(row[i])
                temp_dict[iD] = title
        return temp_dict

    def get_total(self):
        with open('noOfDoc.txt','r') as f:
            return f.readline()

    def load(self,filename):
        temp_list = []
        with bz2.BZ2File(filename,'rb') as f:
            for line in f:
                word = line.strip('\n')
                temp_list.append(word)
        return temp_list

    def print_dic(self):
        print len(self.title_dict)
        for word in self.title_dict.iterkeys():
            print(word,self.title_dict[word][0],self.title_dict[word][1])

    def get_dict(self,flag):
        dic = {}
        if flag == '0':
            dic = self.title_dict
        if flag == '1':
            dic = self.text_dict
        if flag == '2':
            dic = self.info_dict
        if flag == '3':
            dic = self.link_dict
        if flag == '4':
            dic = self.cat_dict
        if flag == '5':
            dic = self.ref_dict
        return dic

    def search(self,word,flag):
        print(word,flag)
        temp_off = self.binSearch(self.offoff_map[flag],self.offset_map[flag],word)
        print(word,flag)
        if temp_off == -1:
            return
        temp_f = bz2.BZ2File(self.offset_map[flag],'rb')
        temp_f.seek(temp_off,0)
        dic = temp_f.readline()
        dic = dic.split(' ')
        if word == dic[0]:
            fileno = dic[1].strip('\n')
            off_file = self.fieldOff_map[flag] + str(fileno) + '.txt.bz2'
            ser_file = self.field_map[flag] + str(fileno) + '.txt.bz2'
            offset = self.binSearch(off_file,ser_file,word)
            f = bz2.BZ2File(ser_file, 'rb')
            f.seek(int(offset))
            term_freqs = f.readline().split(' ')[1].strip('\n').split('|')
            idf = len(term_freqs)
            for term_freq in term_freqs:
                term_freq = term_freq.strip('\n')
                docid = term_freq.split('_')[0]
                tf = term_freq.split('_')[1]
                weight = (1 + math.log(float(tf)))*(math.log(float(self.total)/float(idf)))
                if docid in self.net_score:
                    self.net_score[docid][int(flag)] += weight
                else:
                    self.net_score[docid] = [0,0,0,0,0,0]
                    self.net_score[docid][int(flag)] += weight


    def take(self):
        print("\nSTART SEARCHING : \n")
        while(1):
            self.net_score.clear()
            what = raw_input()
            startTime = datetime.datetime.now()
            if what == "quit":
                break
            if re.search(r'[T|B|I|L|C|R]:',what[:2]):
                previousField = ""
                what = what.strip().split(' ')
                for word in what:
                    if word[1] == ':':
                        previousField = word[0]
                    term = word[2:]
                    term = stem.stemWord(term)
                    #print(term,field)
                    self.search(term,self.field_flag[previousField])
                endTime = datetime.datetime.now()


            else:
                #what = what.strip().split(' ')
                what = self.tokenize(what)
                for word in what:
                    for i in xrange(1,2):
                        self.search(word,str(i))
                endTime = datetime.datetime.now()

            result = {}
            for docid in self.net_score:
                result[docid] = self.net_score[docid][0]*(0.40)
                result[docid] += self.net_score[docid][1]*(0.20)
                result[docid] += self.net_score[docid][2]*(0.10)
                result[docid] += self.net_score[docid][3]*(0.10)
                result[docid] += self.net_score[docid][4]*(0.10)
                result[docid] += self.net_score[docid][5]*(0.10)

            if not result:
                print "\nTIME TAKEN : ",(endTime - startTime).total_seconds()
                print ("\nquery not found\n")
            else:
                print "\nTIME TAKEN : ",(endTime - startTime).total_seconds()
                sorted_x = sorted(result.items(), key=operator.itemgetter(1))
                sorted_x.reverse()
                i = 0
                print("\n")
                for docid in sorted_x:
                    print "%s : " % docid[0],
                    print (' '.join(self.id_title[docid[0]]))
                    i += 1
                    if(i >= 10):
                        break;


if __name__ == "__main__":
    wiki = query()
    print("Please wait...")
    wiki.preprocess()

    wiki.take()
