package Ass1;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.amazonaws.services.codedeploy.model.InstanceType;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class LocalApp {
	private static String id;
	private static String inputFileName;
	private static String outputFileName;
	private static int fileToWorkers;
	private static boolean shoudTerminate;
	private static final String BUCKET_NAME = "ssteinbigbez";
	private static String objectName;
	private static String upstreamURL;
	private static String downstreamURL;
	private static final String URLS_FILENAME = "URLsFile.txt";
	private static AmazonS3 s3;
	private static AmazonEC2 ec2;
	private static AmazonSQS sqs;
	private static AWSCredentials credentials;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		  if (args.length < 3 || args.length > 4) {
	            System.out.println("Expecting: java -jar <jarName.jar> <inputFileName.txt> <outputFileName.html> <workersToFileRatio> <terminate>");
	            System.exit(1);
		  }
		  
		  inputFileName = args[0];
	      outputFileName = args[1];
	      fileToWorkers = Integer.parseInt(args[2]);
	      shoudTerminate = args.length == 4 && args[3].equals("terminate");
	      id = UUID.randomUUID().toString();
	      objectName = id + inputFileName;
	      System.out.println("ID of Local Instance: " + id);
	      
	      /*
	         * The ProfileCredentialsProvider will return your [default]
	         * credential profile by reading from the credentials file located at
	         * (C:\\Users\\matan bezen\\.aws\\credentials).
	         */
          AWSCredentials credentials = null;
	      try {
	          credentials = new ProfileCredentialsProvider("default").getCredentials();
	      } catch (Exception e) {
          throw new AmazonClientException(
                   "Cannot load the credentials from the credential profiles file. " +
                   "Please make sure that your credentials file is at the correct " +
                   "location (C:\\Users\\matan bezen\\.aws\\credentials), and is in valid format.",
                   e);
	      }
	      
	      //1. Local Application uploads the file with the list of PDF files and operations to S3.
	      AmazonS3 s3 = new AmazonS3Client(credentials);
	      Region usEast1 = Region.getRegion(Regions.US_EAST_1);
	      s3.setRegion(usEast1);
	      System.out.println("=========create bucket " + BUCKET_NAME + "=========");
	      s3.createBucket(BUCKET_NAME);
	      
	      System.out.print("Uploading the PDF Document links file to S3... ");
          File file = new File(inputFileName);
          s3.putObject(new PutObjectRequest(BUCKET_NAME, objectName, file));
          System.out.println("Done.");
          
          //initialize the sqs queue
          sqs = new AmazonSQSClient(credentials);
          sqs.setRegion(usEast1);
          
          
          //Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node
          ec2 = new AmazonEC2Client(credentials);
          ec2.setRegion(usEast1);
          
          
          List<String> tagValues = new ArrayList<String>();
          tagValues.add("Manager");
          Filter tagFilter = new Filter("tag:Shahaf", tagValues);
          List<String> statusValues = new ArrayList<String>();
          statusValues.add("running");
          Filter statusFilter = new Filter("instance-state-name", statusValues);
          DescribeInstancesResult filteredInstances = ec2.describeInstances(new DescribeInstancesRequest().withFilters(tagFilter, statusFilter));
          List<Reservation> reservations = filteredInstances.getReservations();
          if (reservations.size() > 0) { // a Manager instance is already running
              // get the URLs file from S3
              System.out.print("Manager instance already running, downloading UPSTREAM\\DOWNSTREAM queues' URLs file from S3... ");
              S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
              System.out.println("Done.");
              BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
              upstreamURL = br.readLine();
              downstreamURL = br.readLine();
              br.close();
          }
          else { // create the SQSs and start a manager instance

              System.out.println("No Manager instance currently running.");
              // start 2 SQSs: upstream, downstream
              try {
                  // Create the upstream and downstream queues
                  System.out.print("Creating upstream queue... ");
                  CreateQueueRequest createUpstreamQueueRequest = new CreateQueueRequest("upstream");
                  upstreamURL = sqs.createQueue(createUpstreamQueueRequest).getQueueUrl();
                  System.out.println("Done.");
                  System.out.print("Creating downstream queue... ");
                  CreateQueueRequest createDownstreamQueueRequest = new CreateQueueRequest("downstream");
                  downstreamURL = sqs.createQueue(createDownstreamQueueRequest).getQueueUrl();
                  System.out.println("Done.");

                  // create a file that holds the queues' URLs, and upload it to S3 for the manager
                  File fileUrl = new File(System.getProperty("user.dir") + "/" + URLS_FILENAME);
                  FileWriter fw = new FileWriter(file);
                  fw.write(upstreamURL + "\n");
                  fw.write(downstreamURL + "\n");
                  fw.write(Integer.toString(fileToWorkers) + "\n");
                  fw.close();
                  System.out.print("Uploading the UPSTREAM\\DOWNSTREAM queues' URLs file to S3... ");
                  s3.putObject(new PutObjectRequest(BUCKET_NAME, URLS_FILENAME, file));
                  System.out.println("Done.");
              } catch (AmazonServiceException ase) {
                  System.out.println("Caught an AmazonServiceException, which means your request made it " +
                          "to Amazon SQS, but was rejected with an error response for some reason.");
                  System.out.println("Error Message:    " + ase.getMessage());
                  System.out.println("HTTP Status Code: " + ase.getStatusCode());
                  System.out.println("AWS Error Code:   " + ase.getErrorCode());
                  System.out.println("Error Type:       " + ase.getErrorType());
                  System.out.println("Request ID:       " + ase.getRequestId());
              } catch (AmazonClientException ace) {
                  System.out.println("Caught an AmazonClientException, which means the client encountered " +
                          "a serious internal problem while trying to communicate with SQS, such as not " +
                          "being able to access the network.");
                  System.out.println("Error Message: " + ace.getMessage());
              }

              // start a Manager instance
              try {
                  System.out.println("Firing up new Manager instance...");
                  RunInstancesRequest request = new RunInstancesRequest("ami-37d0c45d", 1, 1); // upgraded ami: yum updated, java 8
                  request.setInstanceType(InstanceType.T2Micro.toString());
                  request.setUserData(getUserDataScript());
                  IamInstanceProfileSpecification iamInstanceProfileSpecification = new IamInstanceProfileSpecification();
                  iamInstanceProfileSpecification.setName("creds");
                  request.setIamInstanceProfile(iamInstanceProfileSpecification);
                  List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
                  System.out.println("Launch instances: " + instances);
                  CreateTagsRequest createTagRequest = new CreateTagsRequest();
                  createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("shahaf", "manager"));
                  ec2.createTags(createTagRequest);
              } catch (AmazonServiceException ase) {
                  System.out.println("Caught Exception: " + ase.getMessage());
                  System.out.println("Response Status Code: " + ase.getStatusCode());
                  System.out.println("Error Code: " + ase.getErrorCode());
                  System.out.println("Request ID: " + ase.getRequestId());
              }
          }
          //sending the location of the pdf`s links
          sqs.sendMessage(upstreamURL, "fileLocation-" + objectName);

          // wait for the manager to run
          DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
          while (ec2.describeInstances(describeInstancesRequest).getReservations().isEmpty()) {
              try {
                  sleep(1000);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
          System.out.println("Manager instance running.");
          compileResultsToHTML();
      }
	
	
          System.out.println("Deleting an object\n");
          s3.deleteObject(BUCKET_NAME, objectName);

		  System.exit(0);
	}
}
