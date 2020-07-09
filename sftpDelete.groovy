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

def flowFile = session.get()
if(!flowFile) return
try
{
JSch jsch = new JSch();
// get session
nasUser=flowFile.getAttribute('nasuser')
nasIP=flowFile.getAttribute('nasip')
landingPath=flowFile.getAttribute('landingPath')
filePattern=flowFile.getAttribute('filepattern')
keyPath=flowFile.getAttribute('rsaKeyPath')

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
		throw new FileNotFound("file not found : "+filePattern+"  "+e.toString())
}
for (Object obj : files) {
file=String.format(obj.getFilename())
        if (  file == '.' || file == '..' || file.contains('filepart')){
// ignore the current and parent directories
//              println ('File: '+file)
        }
        else{
				sftp.rm(landingPath+'/'+filePattern)
}
}
// disconnect the sftp channel and session
session.transfer(flowFile, REL_SUCCESS)
sftp.disconnect();
jsession.disconnect();
}
catch(FileNotFound e){
        flowFile=session.putAttribute(flowFile,"error_msg",e.toString())
		session.transfer(flowFile,REL_SUCCESS)
}
catch(Exception e)
{
// in case of fire, pull, push and over n out
		flowFile=session.putAttribute(flowFile,"error_msg",e.toString()+" "+e.getStackTrace())
		session.transfer(flowFile,REL_FAILURE)
}
