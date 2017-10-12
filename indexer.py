#!/usr/bin/env python
import re
import sys
import xml.sax
from Stemmer import Stemmer
from unidecode import unidecode
import unicodedata
import bz2

stem=Stemmer('english')

fileno = -1
indexfile =  "./index/"
idfile = open("./id_title.txt","w")
N = open("./noOfDoc.txt","w")
total = 0
class MovieHandler(xml.sax.ContentHandler ):
    def __init__(self):
        self.tag = ""
        self.title = []
        self.id = "0"
        self.invert_dic = {}
        self.text = []
        self.sw = {}
        self.pw = []
        self.links = []
        self.references = []
        self.categories = []
        self.info = []
        self.link_flag = 0
        self.references_flag = 0
        self.categories_flag = 0
        self.info_flag = 0
        self.getStopwords()
        self.title_id = {}

    def getStopwords(self):
        f=open('stopwordsfile.txt', 'r')
        stopwords=[line.rstrip() for line in f]
        self.sw=dict.fromkeys(stopwords)
        f.close()

    def initialize(self):
        self.title = []
        self.id = "0"
        self.text = []
        self.links = []
        self.references = []
        self.categories = []
        self.info = []
        self.link_flag = 0
        self.references_flag = 0
        self.categories_flag = 0
        self.info_flag = 0

    # Call when an element starts
    def startElement(self, tag, attributes):
        self.tag = tag
        if self.tag == "page":
            self.initialize()
            #print "page started"


    def tokenize(self,line):
        line = line.lower()
        newline = re.sub('[^a-z]',' ', line)
        line = newline.split()
        #line = re.findall("\d+|[\w]+",line)
        newline = [key.encode('utf-8') for key in line]
        line = [x for x in newline if x not in self.sw]
        tokens = [stem.stemWord(word) for word in line if len(stem.stemWord(word)) < 13]
        return tokens


    def tokenize_all(self):
        self.tokenize_title()
        self.tokenize_text()

    def store_index(self,lines,index):
        tokens = []
        for line in lines:
            new_line = self.tokenize(line)
            for word in new_line:
                tokens.append(word)
        for token in tokens:
            if (token in self.invert_dic):
                if (self.id in self.invert_dic[token]):
                    self.invert_dic[token][self.id][index] += 1
                else:
                    self.invert_dic[token][self.id] = [0,0,0,0,0,0]
                    self.invert_dic[token][self.id][index] = 1
            else:
                self.invert_dic[token] = {}
                self.invert_dic[token][self.id] = [0,0,0,0,0,0]
                self.invert_dic[token][self.id][index] = 1

    def add_count(self,words,docid,index):
        posting = ""
        if self.invert_dic[words][docid][index] != 0:
            posting += "t"
            posting +=  str(self.invert_dic[words][docid][index])
        return posting


    def create_posting(self,words,docid):
        posting = ""
        if self.invert_dic[words][docid][0] != 0:
            posting += "t"
            posting +=  str(self.invert_dic[words][docid][0])
        if self.invert_dic[words][docid][1] != 0:
	    posting += "x"
            posting +=  str(self.invert_dic[words][docid][1])
        if self.invert_dic[words][docid][2] != 0:
	    posting += "i"
            posting +=  str(self.invert_dic[words][docid][2])
        if self.invert_dic[words][docid][3] != 0:
	    posting += "l"
            posting +=  str(self.invert_dic[words][docid][3])
        if self.invert_dic[words][docid][4] != 0:
	    posting += "c"
            posting +=  str(self.invert_dic[words][docid][4])
        if self.invert_dic[words][docid][5] != 0:
	    posting += "r"
            posting +=  str(self.invert_dic[words][docid][5])
	posting += "|"
        return posting

    def write(self):
        global fileno
        count = 0
        fileno += 1
        global indexfile
        fn = indexfile
        fn += "index_"
        fn += str(fileno)
        fn += ".txt.bz2"
#	output = open("./index/" + fn,"w")
        postings = []
	for words in sorted(self.invert_dic.iterkeys()):
            count+=1
	    posting = words + " "
	    for docid in self.invert_dic[words].iterkeys():
	        posting += docid
                posting += self.create_posting(words,docid)
	    posting = posting[:-1]
            postings.append(posting)
        with bz2.BZ2File(fn, 'wb', compresslevel=9) as f:
	    f.write('\n'.join(postings))

    # Call when an elements ends
    def endElement(self, tag):
        if tag == "page":
            self.store_index(self.title,0)
            self.store_index(self.text,1)
            self.store_index(self.info,2)
            self.store_index(self.links,3)
            self.store_index(self.categories,4)
            self.store_index(self.references,5)

            if sys.getsizeof(self.invert_dic) > 1000*1000:
                self.write()
                self.invert_dic = {}

        elif tag == "mediawiki":
            global total
            for ID in sorted(self.title_id.iterkeys()):
                line = ""
                line += ID
                line += " "
                for subtitle in self.title_id[ID]:
                    line += subtitle
                line += "\n"
                idfile.write(line)
                total += 1
            copy = str(total) + "\n"
            N.write(copy)
            self.write()
            self.invert_dic = {}
        self.tag = ""

    # Call when a character is read
    def characters(self, content):
        content = unicodedata.normalize('NFKD', content).encode('ASCII','ignore')
        content = content.strip()

        if (len(content) == 0):
		return
        if self.tag == "title":
            self.title.append(content)
        elif self.tag == "id":
            if self.id == "0":
                self.id = content
                self.title_id[str(int(self.id))] = self.title
        elif self.tag == "text":
            tempcontent = []
            tempcontent = content.lower()
            self.categories_flag = self.set_cat_flag(content)
            self.info_flag = self.set_info_flag(content)
            self.link_flag = self.set_link_flag(content)
            self.references_flag = self.set_ref_flag(content)

            if self.categories_flag:
                self.categories.append(tempcontent[11:-2])
            elif self.info_flag:
                if (content.find("{{Infobox") != -1):
                    self.info.append(tempcontent[9:])
                else:
                    self.info.append(tempcontent)
            elif self.link_flag:
                if content[0] == '*':
                    self.links.append(tempcontent)
            elif self.references_flag:
                if (content.find("==References==") == -1):
                    self.references.append(tempcontent)
            else:
                self.text.append(tempcontent)

    def set_cat_flag(self,data):
        if(data.find("[[Category:") != -1):
	    return 1
	else:
	    return 0

    def set_info_flag(self,data):
	if(self.info_flag == 0):
	    if(data.find("{{Infobox") != -1):
	        return 1
	    else:
		return 0
	else:
	    if(data == "}}"):
	        return 0
	    else:
		return 1

    def set_link_flag(self,data):
	if(self.link_flag == 0):
	    if(data.find("==External links==") != -1):
	    	return 1
	    else:
	    	return 0
	else:
	    if(data.startswith("==")):
	        return 0
	    else:
	 	return 1

    def set_ref_flag(self,data):
	if(self.references_flag == 0):
	    if(data.find("==References==") != -1):
	        return 1
	    else:
		return 0
        else:
            if(data.find("==") != -1):
                return 0
            else:
                return 1

if __name__ == "__main__":
   
    print ("Please Wait, We will let you know when you can start searching !!")
    xmlfile = sys.argv[1]
    # create an XMLReader
    parser = xml.sax.make_parser()

    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # override the default ContextHandler
    Handler = MovieHandler()

    parser.setContentHandler( Handler )
    #parser.parse("new.xml")
    parser.parse(xmlfile)




