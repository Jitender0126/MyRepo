@Grab(group='com.jcraft', module='jsch', version='0.1.54')
import com.jcraft.jsch.*
import org.apache.ivy.plugins.repository.ssh.SshCache.*
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import org.apache.nifi.processor.io.StreamCallback

class FileNotFound extends Exception {
   public FileNotFound(String msg){
        super(msg)
}
}
// get session
def flowFile = session.get()
if(!flowFile) return
try
{
JSch jsch = new JSch();

//get attributes
nasUser=flowFile.getAttribute('nasuser')
nasIP=flowFile.getAttribute('nasip')
landingPath=flowFile.getAttribute('landingPath')
filePattern=flowFile.getAttribute('filepattern')
keyPath=flowFile.getAttribute('rsaKeyPath')

//create session
Session jsession = jsch.getSession(nasUser, nasIP, 22);
	
//authenticate via RSA key
jsch.addIdentity(keyPath);
Properties config = new Properties();
config.put("StrictHostKeyChecking", "no");
jsession.setConfig(config);
jsession.connect();

//create sftp channel
Channel channel = jsession.openChannel("sftp");
ChannelSftp sftp = (ChannelSftp) channel;
sftp.connect();
String file_list=''
file_name=flowFile.getAttribute('filepattern')
//fetch the list of file based on pattern
Vector files=[]
try{ files = sftp.ls(landingPath+'/'+filePattern)}
catch(Exception e)
{
	sftp.disconnect();
	jsession.disconnect();
	throw new FileNotFound("file not found : "+filePattern+"  "+e.toString())
}
if (files.size()==0){
		sftp.disconnect();
		jsession.disconnect();
		throw new FileNotFound("file not found :"+filePattern)
}
for (Object obj : files) {
file=String.format(obj.getFilename())
        if (  file == '.' || file == '..' || file.contains('filepart')){
// ignore the current and parent directories
//              println ('File: '+file)
        }
        else{
// loop through the file list and create a child flowfile, append attribute
			newFlowFile = session.create(flowFile)
			long fileSize = sftp.lstat(landingPath+'/'+file).getSize()
			newFlowFile = session.putAttribute(newFlowFile, "file_name", file)
			newFlowFile = session.putAttribute(newFlowFile, "file_size", fileSize.toString())

			newFlowFile = session.write(newFlowFile, {outputStream ->
			outputStream.write(file.getBytes())
			} as OutputStreamCallback)

// get the file contents and write to the child flowfile
//			InputStream inputSteam=sftp.get("uploads/hdf-sftp-mx/"+file)
//			newFlowFile = session.write(newFlowFile, {outputStream ->
//			outputStream.write(inputSteam.getBytes())
//			} as OutputStreamCallback)

// route the child flowfile to success relationship
			session.transfer(newFlowFile, REL_SUCCESS)
}
}
//after all child flowfile are routed to success, delete the original flowfile
// disconnect the sftp channel and session
session.remove(flowFile)
sftp.disconnect();
jsession.disconnect();
}
catch(FileNotFound e){
        flowFile=session.putAttribute(flowFile,"error_msg",e.toString())
		session.transfer(flowFile,REL_FAILURE)
}
catch(Exception e)
{
// in case of fire
		flowFile=session.putAttribute(flowFile,"error_msg",e.toString()+" "+e.getStackTrace())
		session.remove(newFlowFile)
		session.transfer(flowFile,REL_FAILURE)
}
