package Ass1;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.simpleworkflow.flow.worker.SynchronousActivityTaskPoller;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import static java.lang.Thread.sleep;

public class Worker {
	private static final String BUCKET_NAME = "ssteinbigbez";
	private static final String WORKER_QUEUES_FILENAME = "QueueWorkerURLS.txt";
	private static String jobsURLQueue;
	private static String resultsURLQueue;
	public static String workerId;
	private static AmazonS3 s3;
	private static AmazonEC2 ec2;
	private static AmazonSQS sqs;

	public static void main(String[] args) throws IOException {
		
        initWorker();

        // get the  SQS URLs file from S3
        System.out.print("Downloading jobs/results queues' URLs file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, WORKER_QUEUES_FILENAME));
        System.out.println("Done.");
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        jobsURLQueue = br.readLine();
        resultsURLQueue = br.readLine();
        br.close();

        // work on the pdfs and add the result to an SQS
        ReceiveMessageResult receiveMessageResult;
        while (true) {
            while ((receiveMessageResult = sqs.receiveMessage(jobsURLQueue)).getMessages().isEmpty()) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            List<Message> messages = receiveMessageResult.getMessages();
            // all incoming jobs have been processed, and the last 'job' is a termination message
            if (messages.size() == 1 && messages.get(0).getBody().contains("terminate")) {
                System.out.println("Termination message received, exiting...");
                Runtime rt = Runtime.getRuntime();
                Process pr = rt.exec("shutdown -h now"); // sends a kill message to the EC2 instance
                break; // if the EC2 instance is still running, this would cause the Worker to end execution
            }
            for (Message message : messages) {
                // made action on the pdfs, create the result message and place it in 'results' SQS
                String job = message.getBody();
                String action = job.substring(job.indexOf("<action>") + 8, job.indexOf("</action>"));
                String link = job.substring(job.indexOf("<link>") + 6, job.indexOf("</link>"));
                PDDocument doc = new PDDocument();
                URL url = new URL(link);
                try {
                	String objectName = UUID.randomUUID().toString();
                	String newLink = "http://s3.amazonaws.com/" + BUCKET_NAME + "/" + objectName;
                    doc = PDDocument.load(url.openStream());
                    File newFile;
                    System.out.println("performed action:" + action + ", on link:" + link);
                    if(action.equals("ToText")){
                    	PDFTextStripper pdfStripper = new PDFTextStripper();
            			pdfStripper.setEndPage(1);
            			String text = pdfStripper.getText(doc);
            			newFile = new File("up.txt");
            			BufferedWriter out = new BufferedWriter(new FileWriter(newFile.getName()));
            			out.write(text);
            			out.close();
                    }
                    else if(action.equals("ToHTML")){
           			 	PDFText2HTML htmlStripper = new PDFText2HTML("UTF8");
           			 	htmlStripper.setEndPage(1);
           			 	htmlStripper.getText(doc);
           			 	String text = htmlStripper.getText(doc);
           			 	newFile = new File("up.html");
           			 	BufferedWriter out = new BufferedWriter(new FileWriter(newFile.getName()));
           			 	out.write(text);
           			 	out.close();
                    }
                    else{
                   	 PDFImageWriter pddIm = new PDFImageWriter();
        			 boolean success = pddIm.writeImage(doc, "png",null , 1, 1, "up");
        			 if(!success)
        				 throw new Exception("image covert did not successed");
        			 newFile = new File("up1.png");
                    }
                    
                    System.out.print("Uploading the convert Document file to S3... ");
      	          	s3.putObject(new PutObjectRequest(BUCKET_NAME, objectName, newFile).withCannedAcl(CannedAccessControlList.PublicRead));
      	          	System.out.println("Done.");
      	          	
                    String result = resultAsString(workerId, link, newLink, action);
                    sqs.sendMessage(resultsURLQueue, result);
                    String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(jobsURLQueue, messageReceiptHandle));
                } catch (Exception e) {
                    sqs.sendMessage(resultsURLQueue, creatErrorMessage(message.getBody(),e.getMessage()));
                    String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(jobsURLQueue, messageReceiptHandle));
                    continue;
                }
            }
        }
    }
	
	private static String creatErrorMessage(String message, String error){
		 StringBuilder sb = new StringBuilder();
		 sb.append("<workerId>");
		 sb.append(workerId);
		 sb.append("</workerId>");
	     sb.append("<message>");
	     sb.append(message);
	     sb.append("</message>");
	     sb.append("<error>");
	     sb.append(error);
      	 sb.append("</error>");
      	 return sb.toString();
	}

    private static void initWorker() {
        workerId = UUID.randomUUID().toString();
        // initiate connection to S3
        s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        
        // initiate connection to SQS
        sqs = new AmazonSQSClient();
        sqs.setRegion(usEast1);

        System.out.println("Worker running...");
    }
    
    private static String resultAsString(String localClientId, String originalURL, String newFileURL, String action) {
        StringBuilder sb = new StringBuilder();
        sb.append("<id>");
        sb.append(localClientId);
        sb.append("</id>");
        sb.append("<originalURL>");
        sb.append(originalURL);
        sb.append("</originalURL>");
        sb.append("<newFileURL>");
        sb.append(newFileURL);
        sb.append("</newFileURL>");
        sb.append("<action>");
        sb.append(action);
        sb.append("</action>");
        return sb.toString();
    }
}
