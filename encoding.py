import json,csv,io,sys,traceback
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
from java.io import BufferedReader
from io import StringIO
import json,re

class PyStreamCallback(StreamCallback):
    def __init__(self, flowfile):
        self.ff = flowfile
        pass
    def process(self, inputStream, outputStream):
       
#        csv_writer=csv.writer(outputStream)
        self.delimit =self.ff.getAttribute('delimiter')
        self.escape_char =self.ff.getAttribute('escape_char')
        fields =self.ff.getAttribute('fields')
        self.quote_char =self.ff.getAttribute('quote_char')
        self.encod =self.ff.getAttribute('file_encoding')
        schema_length =self.ff.getAttribute('schema_length')
        file_type =self.ff.getAttribute('file_type')
        
        if str(self.encod).lower()=='utf8':
            self.encod=StandardCharsets.UTF_8
        elif str(self.encod).lower()=='iso-8859-1':
            self.encod=StandardCharsets.ISO_8859_1
        elif str(self.encod).lower()=='us-ascii':
            self.encod=StandardCharsets.US_ASCII
        elif str(self.encod).lower()=='cp1252':
            self.encod=Charset.forName("windows-1252")

        file_data=''
        count=0
        for data in IOUtils.readLines(inputStream,self.encod):
            file_data=file_data+'\n'+data
            file=StringIO(file_data)
        str_list=[]
        if file_type=='Delimited':
            csv_writer=csv.writer(outputStream,delimiter=str(self.delimit),escapechar=str(self.escape_char),quotechar=str(self.quote_char),quoting=csv.QUOTE_MINIMAL)
            field_list=json.loads(fields)
#                IOUtils.write(str(x), outputStream, StandardCharsets.UTF_8)
#            file = IOUtils.readLines(inputStream, StandardCharsets.UTF_8)
            data=''
            csv_reader=csv.reader(file,delimiter=str(self.delimit),escapechar=str(self.escape_char),quotechar=str(self.quote_char))
            next(csv_reader)
            for line in csv_reader:
                count+=1
                row=list(line[x[0]].strip() if ('int','double','float','decimal','long') in x[1]['type'] else line[x[0]] for x in enumerate(field_list['fields']))
                row=[str(item).encode('utf8').decode('utf8') for item in row]
                csv_writer.writerow(row)
        if file_type=='Fixed Width':
            field_list=fields.split(')|(')
            len_list=list(map(lambda x:x.split(',')[1:4],field_list))
            len_list=[[i[0],i[i],i[2].replace("'",'')] for i in len_list]
            csv_writer=csv.writer(outputStream,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL)
            for line in file:
                count=count+1
                line=data.splitlines()[0]
                if int(schema_length.strip()) != int(len(line))
                    raise Exception('Schema length validation failed for line :'+str(count)+', line length :'+str(len(line))+' Expected length : '+str(schema_length))
                row=list([line[int(x[0])-1:(int(x[1])+int(x[0])-1)].strip() if x[2].lower() in ('int','double','float','decimal','long') else line[int(x[0])-1:(int(x[1])+int(x[0])-1)] for x in len_list])
                row=[str(item).encode('utf8').decode('utf8') for item in row]
                csv_writer(row)

flowFile = session.get()
delimit=flowFile.getAttribute('delimiter')
if (flowFile != None):
    try:
        flowFile = session.write(flowFile,PyStreamCallback(flowFile))
        session.transfer(flowFile, REL_SUCCESS)
    except Exception, err:
        exc_info = sys.exc_info()
        flowFile=session.putAttribute(flowFile,"Error Detail",str(traceback.format_exc()))
        session.transfer(flowFile,REL_FAILURE)
